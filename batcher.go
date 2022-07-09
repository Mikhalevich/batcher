package batcher

import (
	"errors"
	"sync"
	"time"

	"github.com/Mikhalevich/batcher/pkg/logger"
)

// A BatchDoFn is batch worker func.
type BatchDoFn func(data ...interface{}) error

// flushRequest contains data that should be cleared in batch.
type flushRequest struct {
	data []interface{}
}

// ErrStopped is the error returned when batch is stopped.
var ErrStopped = errors.New("batch closed")

type batcher struct {
	// batch data storage.
	items []interface{}
	// doFn handler used for batch data processing by workers.
	doFn BatchDoFn

	// used for waiting until all workers completed before stop the batch.
	workersGroup sync.WaitGroup

	// used for sending flushRequest to workers.
	reqChan chan flushRequest
	// batch state flag shows whether batch is ready to accept data.
	isRunning bool

	// used for inserting new data into batch.
	insertChan chan interface{}

	opts options
}

// New creates new batcher.
func New(name string, flushHandler BatchDoFn, opts ...option) *batcher {
	defaultOptions := options{
		MaxBatchSize:    10,
		MaxWaitInterval: time.Second * 10,
		WorkersCount:    4,
		Logger:          logger.NewNullWrapper(),
	}

	for _, o := range opts {
		o(&defaultOptions)
	}

	defaultOptions.Logger = defaultOptions.Logger.WithField("worker_name", name)

	b := batcher{
		items:        make([]interface{}, 0, defaultOptions.MaxBatchSize+1),
		doFn:         flushHandler,
		workersGroup: sync.WaitGroup{},
		reqChan:      make(chan flushRequest, defaultOptions.WorkersCount),
		isRunning:    true,
		insertChan:   make(chan interface{}, defaultOptions.MaxBatchSize),
		opts:         defaultOptions,
	}

	b.runFlushWorkers()

	go b.run()

	return &b
}

func (b *batcher) runFlushWorkers() {
	for id := 1; id <= b.opts.WorkersCount; id++ {
		b.workersGroup.Add(1)

		go func(workerID int, req <-chan flushRequest) {
			defer b.workersGroup.Done()

			for r := range req {
				if err := b.doFn(r.data...); err != nil {
					b.opts.Logger.
						WithField("worker_id", workerID).
						WithError(err).
						Error("failed to flush task")
				}
			}
		}(id, b.reqChan)
	}
}

func (b *batcher) Insert(data ...interface{}) error {
	if !b.isRunning {
		return ErrStopped
	}

	for _, d := range data {
		b.insertChan <- d
	}

	return nil
}

func (b *batcher) Stop() error {
	if !b.isRunning {
		return ErrStopped
	}

	close(b.insertChan)
	b.workersGroup.Wait()
	return nil
}

func (b *batcher) sendFlushRequest() {
	b.reqChan <- flushRequest{
		data: append([]interface{}{}, b.items...),
	}

	b.items = b.items[:0]
}

func (b *batcher) run() {
	b.opts.Logger.Info("worker started")
	defer b.opts.Logger.Info("worker stopped")

	t := time.NewTicker(b.opts.MaxWaitInterval)
	defer t.Stop()

	defer func() {
		if len(b.items) > 0 {
			b.sendFlushRequest()
		}
		close(b.reqChan)
	}()

	for {
		select {
		case <-t.C:
			if len(b.items) > 0 {
				b.sendFlushRequest()
			}
		case item, ok := <-b.insertChan:
			if !ok {
				b.isRunning = false
				return
			}

			b.items = append(b.items, item)
			if len(b.items) >= b.opts.MaxBatchSize {
				b.sendFlushRequest()
			}
		}
	}
}
