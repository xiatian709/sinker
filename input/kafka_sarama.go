/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package input

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/thanos-io/thanos/pkg/errors"
	"github.com/xdg-go/scram"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/statistics"
	"github.com/housepower/clickhouse_sinker/util"
)

// saramaLogger routes sarama's internal stdlib-style log stream to zap at
// content-appropriate levels. sarama itself has no level concept; without
// classification we either drown logs (route everything at Info) or mislabel
// chatty informational events as errors (route everything at Error). This
// keeps real errors at Error while letting "added subscription" and the like
// be silenced at default Info level.
type saramaLogger struct {
	base *zap.Logger
}

func (s *saramaLogger) Print(v ...interface{})                 { s.dispatch(fmt.Sprint(v...)) }
func (s *saramaLogger) Println(v ...interface{})               { s.dispatch(fmt.Sprintln(v...)) }
func (s *saramaLogger) Printf(format string, v ...interface{}) { s.dispatch(fmt.Sprintf(format, v...)) }

func (s *saramaLogger) dispatch(msg string) {
	msg = strings.TrimRight(msg, "\n")
	if msg == "" {
		return
	}
	switch classifySaramaMsg(msg) {
	case zap.DebugLevel:
		s.base.Debug(msg)
	case zap.InfoLevel:
		s.base.Info(msg)
	case zap.WarnLevel:
		s.base.Warn(msg)
	default:
		s.base.Error(msg)
	}
}

// classifySaramaMsg picks the zap level for a sarama log line based on its
// content. Polarity: default to Info (sarama emits far more lifecycle
// chatter than errors), then upgrade specific keyword hits to Warn/Error
// and demote a small allow-list of high-volume chatter to Debug. The
// previous implementation defaulted to Error, which turned every
// unrecognized variant of a lifecycle message (e.g. "client/brokers
// registered new broker") into a stacktraced error line and drowned the log.
func classifySaramaMsg(msg string) zapcore.Level {
	lower := strings.ToLower(msg)
	// True-error keywords. Case-folded so "Failed to connect" and
	// "failed to connect" both hit. Kept small — every entry here needs to
	// be common enough to be worth the false-positive risk of matching
	// substrings inside a normal message.
	if strings.Contains(lower, "error") ||
		strings.Contains(lower, "failed") ||
		strings.Contains(lower, " eof") ||
		strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "refused") ||
		strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "auth failed") ||
		strings.Contains(lower, "invalid") {
		return zap.ErrorLevel
	}
	// Operational warnings — abnormal but recoverable.
	if strings.Contains(msg, "abandoned subscription") ||
		strings.Contains(msg, "rebalance in progress") ||
		strings.Contains(msg, "resurrecting") ||
		strings.Contains(msg, "retrying") {
		return zap.WarnLevel
	}
	// Very high-volume chatter that fires per-partition or per-fetch. With
	// 300+ partitions this floods Info; demote to Debug.
	if strings.Contains(msg, "added subscription") ||
		strings.Contains(msg, "closed dead subscription") ||
		strings.Contains(msg, "client/metadata fetching") {
		return zap.DebugLevel
	}
	// Everything else — "client/brokers registered", "Connected to broker",
	// "Successful SASL handshake", "Initializing new client",
	// "Closing Client", etc. — is normal lifecycle chatter.
	return zap.InfoLevel
}

// Compile-time check sarama.StdLogger compatibility.
var _ sarama.StdLogger = (*saramaLogger)(nil)

var _ Inputer = (*KafkaSarama)(nil)

// Rebalance-loop detector thresholds. When ClickHouse drain plus the bigger
// session/rebalance timeouts still aren't enough, we want a loud-enough signal
// that an operator can rotate the consumer group manually (e.g. by pushing a
// new taskCfg.ConsumerGroup via Nacos) so all pods rotate in lockstep. Doing
// the rotation locally is unsafe in a multi-pod deployment: each pod would
// pick its own suffix and split the group, multiplying CH writes by the pod
// count.
const (
	rebalanceLoopWindow    = 60 * time.Second
	rebalanceLoopThreshold = 5
	rebalanceLoopReportGap = 60 * time.Second // suppress repeated screaming
)

// KafkaSarama implements input.Inputer
type KafkaSarama struct {
	cfg       *config.Config
	taskCfg   *config.TaskConfig
	cg        sarama.ConsumerGroup
	sess      sarama.ConsumerGroupSession
	ctx       context.Context
	cancel    context.CancelFunc
	wgRun     sync.WaitGroup
	putFn     func(msg *model.InputMessage)
	cleanupFn func()

	// Separate sarama.Client dedicated to admin-style introspection
	// (committed offsets, broker earliest/latest offsets) so we don't have
	// to share the ConsumerGroup's internal client and race against its own
	// OffsetManager. Nil if construction failed — the OOR check then no-ops.
	adminClient sarama.Client

	cleanupMux    sync.Mutex
	cleanupTs     []time.Time // sliding window of recent Cleanup callback timestamps
	loopAlertedAt time.Time   // last time we emitted the loud alert; rate-limit
}

// NewKafkaSarama get instance of kafka reader
func NewKafkaSarama() *KafkaSarama {
	return &KafkaSarama{}
}

type MyConsumerGroupHandler struct {
	k *KafkaSarama //point back to which kafka this handler belongs to
}

func (h MyConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	h.k.sess = sess
	// Setup runs after a successful JoinGroup/SyncGroup, so this is the first
	// point we know our identity in the group and which partitions we own.
	// Equivalent in spirit to logging join.MemberId/GenerationId inside
	// sarama's consumer_group.go.
	util.Logger.Info("consumer group setup",
		zap.String("task", h.k.taskCfg.Name),
		zap.String("consumer group", h.k.taskCfg.ConsumerGroup),
		zap.String("member id", sess.MemberID()),
		zap.Int32("generation id", sess.GenerationID()),
		zap.Any("claims", sess.Claims()))
	// OffsetOutOfRange fallback. Run before any ConsumeClaim goroutine starts
	// so that a partition whose committed offset was already purged by broker
	// retention gets rewound BEFORE the partition consumer tries to fetch it
	// and sarama would otherwise abandon the subscription silently.
	h.k.resetOutOfRangeOffsets(sess)
	return nil
}

// resetOutOfRangeOffsets defends against the "committed offset below broker's
// earliest offset" failure mode. sarama v1.36 does not auto-reset in this
// case: the partition consumer receives OffsetOutOfRange, logs it, and
// abandons the subscription — which then presents as a partition without a
// consumer, triggering rebalance, and repeating forever. Setting
// Consumer.Offsets.Initial does NOT help; that field only applies when there
// is no committed offset at all.
//
// We therefore explicitly detect the condition here and call sess.ResetOffset
// with the broker's earliest (or latest, depending on taskCfg.Earliest) so
// consumption can proceed. Runs at every Setup so the fix also works if a
// partition ages out after startup.
func (k *KafkaSarama) resetOutOfRangeOffsets(sess sarama.ConsumerGroupSession) {
	if k.adminClient == nil {
		return
	}
	om, err := sarama.NewOffsetManagerFromClient(k.taskCfg.ConsumerGroup, k.adminClient)
	if err != nil {
		util.Logger.Warn("could not open OffsetManager for out-of-range check; skipping",
			zap.String("task", k.taskCfg.Name),
			zap.Error(err))
		return
	}
	defer om.Close()

	for topic, partitions := range sess.Claims() {
		for _, partition := range partitions {
			earliest, err := k.adminClient.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				util.Logger.Warn("GetOffset(oldest) failed; skipping partition OOR check",
					zap.String("task", k.taskCfg.Name),
					zap.String("topic", topic),
					zap.Int32("partition", partition),
					zap.Error(err))
				continue
			}
			pom, err := om.ManagePartition(topic, partition)
			if err != nil {
				util.Logger.Warn("ManagePartition failed; skipping partition OOR check",
					zap.String("task", k.taskCfg.Name),
					zap.String("topic", topic),
					zap.Int32("partition", partition),
					zap.Error(err))
				continue
			}
			committed, _ := pom.NextOffset()
			pom.AsyncClose()

			// sarama returns sentinel negatives (OffsetOldest=-2, OffsetNewest=-1)
			// when the coordinator reports no committed offset. Those cases
			// are handled by Consumer.Offsets.Initial and must not trigger a
			// reset here.
			if committed < 0 || committed >= earliest {
				continue
			}
			var resetTo int64
			var policy string
			if k.taskCfg.Earliest {
				resetTo, policy = earliest, "earliest"
			} else {
				latest, lerr := k.adminClient.GetOffset(topic, partition, sarama.OffsetNewest)
				if lerr != nil {
					util.Logger.Warn("GetOffset(newest) failed; falling back to earliest",
						zap.String("task", k.taskCfg.Name),
						zap.String("topic", topic),
						zap.Int32("partition", partition),
						zap.Error(lerr))
					resetTo, policy = earliest, "earliest-fallback"
				} else {
					resetTo, policy = latest, "latest"
				}
			}
			util.Logger.Warn("committed offset out of range on broker; resetting",
				zap.String("task", k.taskCfg.Name),
				zap.String("topic", topic),
				zap.Int32("partition", partition),
				zap.Int64("committed", committed),
				zap.Int64("broker_earliest", earliest),
				zap.Int64("reset_to", resetTo),
				zap.String("policy", policy))
			sess.ResetOffset(topic, partition, resetTo, "reset-out-of-range")
			statistics.OffsetOutOfRangeReset.WithLabelValues(k.taskCfg.Name, topic).Inc()
		}
	}
}

func (h MyConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	begin := time.Now()
	h.k.cleanupFn()
	util.Logger.Info("consumer group cleanup",
		zap.String("task", h.k.taskCfg.Name),
		zap.String("consumer group", h.k.taskCfg.ConsumerGroup),
		zap.String("member id", sess.MemberID()),
		zap.Int32("generation id", sess.GenerationID()),
		zap.Duration("cost", time.Since(begin)))
	h.k.checkRebalanceLoop(begin)
	return nil
}

// checkRebalanceLoop tracks Cleanup invocations in a sliding window. Once they
// cross the threshold, we know we're stuck in a rebalance loop and emit a loud,
// rate-limited alert plus bump a Prometheus counter. We deliberately do NOT
// auto-rotate the consumer group here: in a multi-pod deployment, local-only
// rotation would split the group across pods and explode CH write fan-out.
// Rotation is an operator action (push new taskCfg.ConsumerGroup via Nacos).
func (k *KafkaSarama) checkRebalanceLoop(now time.Time) {
	k.cleanupMux.Lock()
	defer k.cleanupMux.Unlock()
	cutoff := now.Add(-rebalanceLoopWindow)
	// Drop expired entries. The slice never grows beyond threshold+1 (we bail
	// after Inc), so the linear scan is bounded — no perf concern.
	keep := k.cleanupTs[:0]
	for _, ts := range k.cleanupTs {
		if ts.After(cutoff) {
			keep = append(keep, ts)
		}
	}
	keep = append(keep, now)
	k.cleanupTs = keep
	if len(keep) < rebalanceLoopThreshold {
		return
	}
	statistics.RebalanceLoopDetected.WithLabelValues(k.taskCfg.Name).Inc()
	if now.Sub(k.loopAlertedAt) < rebalanceLoopReportGap {
		return
	}
	k.loopAlertedAt = now
	util.Logger.Error("Kafka consumer group rebalance loop detected — pipeline is stuck cycling through Cleanup. "+
		"Rotate the consumer group manually (push a new taskCfg.ConsumerGroup via Nacos) to recover all pods atomically.",
		zap.String("task", k.taskCfg.Name),
		zap.String("consumer group", k.taskCfg.ConsumerGroup),
		zap.Int("cleanups_in_window", len(keep)),
		zap.Duration("window", rebalanceLoopWindow))
}

func (h MyConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	begin := time.Now()
	taskName := h.k.taskCfg.Name
	topic := claim.Topic()
	partition := claim.Partition()
	util.Logger.Info("consumer claim started",
		zap.String("task", taskName),
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("initial offset", claim.InitialOffset()),
		zap.Int32("generation id", sess.GenerationID()))
	var msgCnt int64
	for msg := range claim.Messages() {
		h.k.putFn(&model.InputMessage{
			Topic:     msg.Topic,
			Partition: int(msg.Partition),
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Timestamp: &msg.Timestamp,
		})
		msgCnt++
	}
	// Reaching here means claim.Messages() channel was closed by sarama —
	// either the session ended (rebalance / shutdown) or this partition was
	// revoked. Logging this transition makes it easy to correlate with the
	// "consumer group cleanup" line that follows.
	reason := "channel closed"
	select {
	case <-sess.Context().Done():
		reason = "session ctx done"
	default:
	}
	util.Logger.Info("consumer claim ended",
		zap.String("task", taskName),
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("messages", msgCnt),
		zap.Duration("duration", time.Since(begin)),
		zap.String("reason", reason))
	return nil
}

// Init Initialise the kafka instance with configuration
func (k *KafkaSarama) Init(cfg *config.Config, taskCfg *config.TaskConfig, putFn func(msg *model.InputMessage), cleanupFn func()) (err error) {
	k.cfg = cfg
	k.taskCfg = taskCfg
	k.ctx, k.cancel = context.WithCancel(context.Background())
	k.putFn = putFn
	k.cleanupFn = cleanupFn
	kfkCfg := &cfg.Kafka
	sarCfg, err := GetSaramaConfig(&cfg.Kafka)
	if err != nil {
		return err
	}
	if taskCfg.Earliest {
		sarCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	// IMPORTANT: install the sarama logger BEFORE NewConsumerGroup. sarama's
	// default Logger is io.Discard, so bootstrap failures (EOF, SASL errors,
	// TLS errors) would otherwise be silent.
	//
	// We use a content-classifying logger instead of a flat-level NewStdLogAt:
	// sarama's stream mixes informational lifecycle events ("added subscription")
	// with real errors ("EOF", "SASL Auth failed") and treating them the same
	// either drowns operators in noise or makes routine logs look like alerts.
	// Disable zap stacktraces on the sarama-forwarded logger: the trace only
	// leads back into (*saramaLogger).dispatch, which is pure noise. Real
	// sarama errors already contain the topic/partition/broker context in
	// the message body.
	sarama.Logger = &saramaLogger{
		base: util.Logger.With(zap.String("name", "sarama")).
			WithOptions(zap.AddStacktrace(zapcore.FatalLevel)),
	}
	util.Logger.Info("creating sarama ConsumerGroup",
		zap.String("task", taskCfg.Name),
		zap.String("brokers", kfkCfg.Brokers),
		zap.String("group", taskCfg.ConsumerGroup),
		zap.String("version", kfkCfg.Version),
		zap.Bool("tls", kfkCfg.TLS.Enable),
		zap.Bool("sasl", kfkCfg.Sasl.Enable),
		zap.String("sasl_mechanism", kfkCfg.Sasl.Mechanism))
	cg, err := sarama.NewConsumerGroup(strings.Split(kfkCfg.Brokers, ","), taskCfg.ConsumerGroup, sarCfg)
	if err != nil {
		util.Logger.Error("sarama.NewConsumerGroup failed",
			zap.String("task", taskCfg.Name),
			zap.String("brokers", kfkCfg.Brokers),
			zap.Error(err))
		return err
	}
	k.cg = cg
	// Best-effort admin client. If this fails we still start the pipeline —
	// consumption will just lose the out-of-range self-heal. Not fatal.
	adminCfg, aerr := GetSaramaConfig(&cfg.Kafka)
	if aerr != nil {
		util.Logger.Warn("could not build sarama config for admin client; OOR self-heal disabled",
			zap.String("task", taskCfg.Name), zap.Error(aerr))
	} else {
		adminCfg.ClientID = "clickhouse_sinker_admin"
		adminClient, cerr := sarama.NewClient(strings.Split(kfkCfg.Brokers, ","), adminCfg)
		if cerr != nil {
			util.Logger.Warn("sarama.NewClient (admin) failed; OOR self-heal disabled",
				zap.String("task", taskCfg.Name), zap.Error(cerr))
		} else {
			k.adminClient = adminClient
		}
	}
	return nil
}

func GetSaramaConfig(kfkCfg *config.KafkaConfig) (sarCfg *sarama.Config, err error) {
	sarCfg = sarama.NewConfig()
	if sarCfg.Version, err = sarama.ParseKafkaVersion(kfkCfg.Version); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	if kfkCfg.TLS.Enable {
		sarCfg.Net.TLS.Enable = true
		if sarCfg.Net.TLS.Config, err = util.NewTLSConfig(kfkCfg.TLS.CaCertFiles, kfkCfg.TLS.ClientCertFile, kfkCfg.TLS.ClientKeyFile, kfkCfg.TLS.EndpIdentAlgo == ""); err != nil {
			return
		}
	}
	// check for authentication
	if kfkCfg.Sasl.Enable {
		sarCfg.Net.SASL.Enable = true
		if sarCfg.Version.IsAtLeast(sarama.V1_0_0_0) {
			sarCfg.Net.SASL.Version = sarama.SASLHandshakeV1
		}
		sarCfg.Net.SASL.Mechanism = (sarama.SASLMechanism)(kfkCfg.Sasl.Mechanism)
		switch sarCfg.Net.SASL.Mechanism {
		case "SCRAM-SHA-256":
			sarCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		case "SCRAM-SHA-512":
			// Upstream typo: this branch used SHA256, which caused SCRAM challenge
			// mismatch with brokers configured for SHA-512 (broker EOFs after
			// receiving the wrong client proof).
			sarCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		default:
		}
		sarCfg.Net.SASL.User = kfkCfg.Sasl.Username
		sarCfg.Net.SASL.Password = kfkCfg.Sasl.Password
		sarCfg.Net.SASL.GSSAPI = kfkCfg.Sasl.GSSAPI
	}
	sarCfg.ChannelBufferSize = 1024
	sarCfg.ClientID = "clickhouse_sinker"
	// Consumer.MaxProcessingTime default is 100ms — sarama gives up on a
	// partition and logs "abandoned subscription ... because consuming was
	// taking too long" if our ConsumeClaim doesn't drain a message from the
	// partition channel within that window. With ring lock, parsing-pool
	// submit, and 300+ partitions per task, 100ms is wildly too tight; raise
	// it so partitions aren't churned mid-fetch.
	sarCfg.Consumer.MaxProcessingTime = 5 * time.Second
	// Group timeouts. Defaults (Session 10s / Heartbeat 3s / Rebalance 60s) are too
	// tight when ClickHouse falls behind: drain blocks Cleanup longer than rebalance.timeout
	// and the broker keeps re-revoking partitions, producing infinite "consumer group cleanup"
	// loops. The Rebalance window here must be >= task.Service.drain timeout (Clickhouse.DrainTimeout).
	sarCfg.Consumer.Group.Session.Timeout = 60 * time.Second
	sarCfg.Consumer.Group.Heartbeat.Interval = 15 * time.Second
	sarCfg.Consumer.Group.Rebalance.Timeout = 5 * time.Minute
	return
}

// kafka main loop
func (k *KafkaSarama) Run() {
	k.wgRun.Add(1)
	defer k.wgRun.Done()
	taskCfg := k.taskCfg
	// Exponential backoff so transient broker errors don't degenerate into a tight
	// rejoin spin (which also presents as repeated "consumer group cleanup" logs).
	const minBackoff = time.Second
	const maxBackoff = 30 * time.Second
	backoff := minBackoff
LOOP_SARAMA:
	for {
		select {
		case <-k.ctx.Done():
			break LOOP_SARAMA
		default:
		}
		handler := MyConsumerGroupHandler{k}
		// `Consume` blocks for the lifetime of a session: it triggers JoinGroup,
		// then runs Setup/ConsumeClaim/Cleanup, then returns once the session
		// ends (rebalance, broker disconnect, ctx cancel). Bracket it with logs
		// so we can spot "Consume churn" patterns at a glance.
		consumeBegin := time.Now()
		util.Logger.Info("cg.Consume entering",
			zap.String("task", taskCfg.Name),
			zap.String("topic", taskCfg.Topic),
			zap.String("group", taskCfg.ConsumerGroup))
		err := k.cg.Consume(k.ctx, []string{taskCfg.Topic}, handler)
		util.Logger.Info("cg.Consume returned",
			zap.String("task", taskCfg.Name),
			zap.Duration("session_lifetime", time.Since(consumeBegin)),
			zap.Error(err))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, sarama.ErrClosedConsumerGroup) {
				break LOOP_SARAMA
			}
			statistics.ConsumeMsgsErrorTotal.WithLabelValues(taskCfg.Name).Inc()
			err = errors.Wrapf(err, "")
			util.Logger.Error("sarama.ConsumerGroup.Consume failed, backing off",
				zap.String("task", k.taskCfg.Name),
				zap.Duration("backoff", backoff),
				zap.Error(err))
			select {
			case <-k.ctx.Done():
				break LOOP_SARAMA
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}
		backoff = minBackoff
	}
	k.cg.Close()
	util.Logger.Info("KafkaSarama.Run quit due to context has been canceled", zap.String("task", k.taskCfg.Name))
}

func (k *KafkaSarama) CommitMessages(msg *model.InputMessage) error {
	k.sess.MarkOffset(msg.Topic, int32(msg.Partition), msg.Offset+1, "")
	return nil
}

// Stop kafka consumer and close all connections
func (k *KafkaSarama) Stop() error {
	k.cancel()
	k.wgRun.Wait()
	if k.adminClient != nil {
		_ = k.adminClient.Close()
	}
	return nil
}

// Description of this kafka consumer, which topic it reads from
func (k *KafkaSarama) Description() string {
	return "kafka consumer of topic " + k.taskCfg.Topic
}

// Predefined SCRAMClientGeneratorFunc, copied from https://github.com/Shopify/sarama/blob/master/examples/sasl_scram_client/scram_client.go

var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
