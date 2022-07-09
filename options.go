package batcher

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Mikhalevich/batcher/pkg/logger"
)

type options struct {
	// MaxBatchSize the limit when batch reaches this limit it will be cleared (if 1 or 0 is passed - data will be processed immediately).
	MaxBatchSize int
	// max batch clearing interval.
	MaxWaitInterval time.Duration
	// number of workers
	WorkersCount int
	// prepared logger
	Logger logger.Logger
}

type option func(opts *options)

func WithMaxBatchSize(size int) option {
	return func(opts *options) {
		opts.MaxBatchSize = size
	}
}

func WithMaxWaitInterval(t time.Duration) option {
	return func(opts *options) {
		opts.MaxWaitInterval = t
	}
}

func WithWorkersCount(count int) option {
	return func(opts *options) {
		opts.WorkersCount = count
	}
}

func WithLogger(logger logger.Logger) option {
	return func(opts *options) {
		opts.Logger = logger
	}
}

func WithLogrusLogger(log *logrus.Logger) option {
	return func(opts *options) {
		opts.Logger = logger.NewLogrusWrapper(log)
	}
}
