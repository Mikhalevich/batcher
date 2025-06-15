package batcher

import (
	"errors"
	"sync"
	"time"

	"github.com/Mikhalevich/batcher/pkg/logger"
)

const (
	defaultMaxBatchSize    = 10
	defaultMaxWaitInterval = time.Second * 10
	defaultWorkersCount    = 4
)

// A BatchDoFn is batch worker func.
type BatchDoFn[T any] func(data ...T) error

// flushRequest contains data that should be cleared in batch.
type flushRequest[T any] struct {
	data []T
}

// ErrStopped is the error returned when batch is stopped.
var ErrStopped = errors.New("batcher is closed")

type batcher[T any] struct {
	// batch data storage.
	items []T
	// doFn handler used for batch data processing by workers.
	doFn BatchDoFn[T]

	// used for waiting until all workers completed before stop the batch.
	workersGroup sync.WaitGroup

	// used for sending flushRequest to workers.
	reqChan chan flushRequest[T]
	// batch state flag shows whether batch is ready to accept data.
	isRunning  bool
	runningMtx sync.RWMutex

	// used for inserting new data into batch.
	insertChan chan T

	opts options
}

// New creates new batcher.
func New[T any](name string, flushHandler BatchDoFn[T], opts ...option) *batcher[T] {
	defaultOptions := options{
		MaxBatchSize:    defaultMaxBatchSize,
		MaxWaitInterval: defaultMaxWaitInterval,
		WorkersCount:    defaultWorkersCount,
		Logger:          logger.NewNullWrapper(),
	}

	for _, o := range opts {
		o(&defaultOptions)
	}

	defaultOptions.Logger = defaultOptions.Logger.WithField("worker_name", name)

	bat := batcher[T]{
		items:        make([]T, 0, defaultOptions.MaxBatchSize+1),
		doFn:         flushHandler,
		workersGroup: sync.WaitGroup{},
		reqChan:      make(chan flushRequest[T], defaultOptions.WorkersCount),
		isRunning:    true,
		insertChan:   make(chan T, defaultOptions.MaxBatchSize),
		opts:         defaultOptions,
	}

	bat.runFlushWorkers()

	go bat.run()

	return &bat
}

func (b *batcher[T]) runFlushWorkers() {
	for workerID := 1; workerID <= b.opts.WorkersCount; workerID++ {
		b.workersGroup.Add(1)

		go func(workerID int, req <-chan flushRequest[T]) {
			defer b.workersGroup.Done()

			for r := range req {
				if err := b.doFn(r.data...); err != nil {
					b.opts.Logger.
						WithField("worker_id", workerID).
						WithError(err).
						Error("failed to flush task")
				}
			}
		}(workerID, b.reqChan)
	}
}

func (b *batcher[T]) Insert(data ...T) error {
	b.runningMtx.RLock()
	defer b.runningMtx.RUnlock()

	if !b.isRunning {
		return ErrStopped
	}

	for _, d := range data {
		b.insertChan <- d
	}

	return nil
}

func (b *batcher[T]) Stop() error {
	b.runningMtx.Lock()
	defer b.runningMtx.Unlock()

	if !b.isRunning {
		return ErrStopped
	}

	b.isRunning = false

	close(b.insertChan)
	b.workersGroup.Wait()

	return nil
}

func (b *batcher[T]) sendFlushRequest() {
	b.reqChan <- flushRequest[T]{
		data: append([]T{}, b.items...),
	}

	b.items = b.items[:0]
}

func (b *batcher[T]) run() {
	b.opts.Logger.Info("worker started")
	defer b.opts.Logger.Info("worker stopped")

	ticker := time.NewTicker(b.opts.MaxWaitInterval)
	defer ticker.Stop()

	defer func() {
		if len(b.items) > 0 {
			b.sendFlushRequest()
		}

		close(b.reqChan)
	}()

	for {
		select {
		case <-ticker.C:
			if len(b.items) > 0 {
				b.sendFlushRequest()
			}

		case item, ok := <-b.insertChan:
			if !ok {
				return
			}

			b.items = append(b.items, item)
			if len(b.items) >= b.opts.MaxBatchSize {
				b.sendFlushRequest()
			}
		}
	}
}
