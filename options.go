package batcher

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Mikhalevich/batcher/logger"
)

type options struct {
	// MaxBatchSize the limit when batch reaches this limit it will be cleared.
	// (if 1 or 0 is passed - data will be processed immediately).
	MaxBatchSize int
	// time duration interval before flushing processing.
	MaxWaitInterval time.Duration
	// number of workers
	WorkersCount int
	// prepared logger
	Logger logger.Logger
}

// Option custom option for batcher.
type Option func(opts *options)

// WithMaxBatchSize set limit of processing bucket.
func WithMaxBatchSize(size int) Option {
	return func(opts *options) {
		opts.MaxBatchSize = size
	}
}

// WithMaxWaitInterval set periodic batch processing time interval.
func WithMaxWaitInterval(t time.Duration) Option {
	return func(opts *options) {
		opts.MaxWaitInterval = t
	}
}

// WithWorkersCount set parallel batch processing workers.
func WithWorkersCount(count int) Option {
	return func(opts *options) {
		opts.WorkersCount = count
	}
}

// WithLogger set logger for batch worker.
func WithLogger(logger logger.Logger) Option {
	return func(opts *options) {
		opts.Logger = logger
	}
}

// WithLogrusLogger set logrus loggger for batch worker.
func WithLogrusLogger(log *logrus.Logger) Option {
	return func(opts *options) {
		opts.Logger = logger.NewLogrus(log)
	}
}
