package util

import (
	"errors"
	"sync"
	"sync/atomic"
)

const (
	StateRunning uint32 = 0
	StateStopped uint32 = 1
)

// WorkerPool is a blocked worker pool inspired by https://github.com/gammazero/workerpool/
type WorkerPool struct {
	inNums     uint64
	outNums    uint64
	curWorkers int

	maxWorkers int
	workChan   chan func()

	taskDone *sync.Cond
	state    uint32
	sync.Mutex
}

// New creates and starts a pool of worker goroutines.
func NewWorkerPool(maxWorkers int, queueSize int) *WorkerPool {
	if maxWorkers <= 0 {
		Logger.Fatal("WorkerNum must be greater than zero")
	}
	if queueSize <= 0 {
		Logger.Fatal("queueSize must be greater than zero")
	}

	w := &WorkerPool{
		maxWorkers: maxWorkers,
		workChan:   make(chan func(), queueSize),
	}

	w.taskDone = sync.NewCond(w)

	w.start()
	return w
}

var (
	// ErrStopped when stopped
	ErrStopped = errors.New("WorkerPool already stopped")
)

func (w *WorkerPool) wokerFunc() {
	w.Lock()
	w.curWorkers++
	w.Unlock()
LOOP:
	for fn := range w.workChan {
		fn()
		var needQuit bool
		w.Lock()
		w.outNums++
		if w.inNums == w.outNums {
			w.taskDone.Signal()
		}
		if w.curWorkers > w.maxWorkers {
			w.curWorkers--
			needQuit = true
		}
		w.Unlock()
		if needQuit {
			break LOOP
		}
	}
}

func (w *WorkerPool) start() {
	for i := 0; i < w.maxWorkers; i++ {
		go w.wokerFunc()
	}
}

// Resize ensures worker number match the expected one.
func (w *WorkerPool) Resize(maxWorkers int) {
	w.Lock()
	defer w.Unlock()
	for i := 0; i < maxWorkers-w.maxWorkers; i++ {
		go w.wokerFunc()
	}
	w.maxWorkers = maxWorkers
	// if maxWorkers<w.maxWorkers, redundant workers quit by themselves
}

// Submit enqueues a function for a worker to execute.
// Submit will block regardless if there is no free workers.
func (w *WorkerPool) Submit(fn func()) (err error) {
	if atomic.LoadUint32(&w.state) == StateStopped {
		return ErrStopped
	}

	w.Lock()
	w.inNums++
	w.Unlock()

	w.workChan <- fn
	return nil
}

// TrySubmit enqueues fn without blocking. Returns true on success, false if the
// queue is full or the pool is stopped. Callers use the false return to apply
// backpressure-free fast-fail (e.g. drop the batch and advance Kafka offset)
// instead of blocking upstream, which is essential for keeping the Kafka
// consumer responsive when ClickHouse falls behind.
func (w *WorkerPool) TrySubmit(fn func()) (accepted bool) {
	if atomic.LoadUint32(&w.state) == StateStopped {
		return false
	}
	w.Lock()
	w.inNums++
	w.Unlock()
	select {
	case w.workChan <- fn:
		return true
	default:
		w.Lock()
		w.inNums--
		w.Unlock()
		return false
	}
}

// Backlog reports the current number of queued, not-yet-running tasks. Used
// for diagnostic logging only — racy by design.
func (w *WorkerPool) Backlog() int {
	return len(w.workChan)
}

// StopWait stops the worker pool and waits for all queued tasks tasks to complete.
func (w *WorkerPool) StopWait() {
	atomic.StoreUint32(&w.state, StateStopped)

	w.Lock()
	defer w.Unlock()
	for w.inNums != w.outNums {
		w.taskDone.Wait()
	}
}

func (w *WorkerPool) Restart() {
	atomic.StoreUint32(&w.state, StateRunning)
}
