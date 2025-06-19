package batcher_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/Mikhalevich/batcher"
)

func generateTestData(count int) []interface{} {
	testData := make([]interface{}, 0, count)
	for i := range count {
		testData = append(testData, i)
	}

	return testData
}

func Test_BatchInsertOneByOne(t *testing.T) {
	t.Parallel()

	var (
		totalProcessed int64
		testData       = generateTestData(20)
		mut            sync.Mutex
		collected      = make([]interface{}, 0, len(testData))
	)

	bat := batcher.New(func(datas ...interface{}) error {
		mut.Lock()
		collected = append(collected, datas...)
		mut.Unlock()
		atomic.AddInt64(&totalProcessed, int64(len(datas)))

		return nil
	},
		batcher.WithMaxBatchSize(10),
		batcher.WithMaxWaitInterval(300*time.Millisecond),
		batcher.WithWorkersCount(3),
		batcher.WithLogrusLogger(logrus.StandardLogger()),
	)

	for _, d := range testData {
		err := bat.Insert(d)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	err := bat.Stop()
	require.NoError(t, err)

	processed := atomic.LoadInt64(&totalProcessed)
	expectedProcessed := int64(len(testData))
	require.Equal(t, expectedProcessed, processed)

	require.ElementsMatch(t, testData, collected, "testdata [%v]\n collected [%v]", testData, collected)
}

func Test_BatchInsertList(t *testing.T) {
	t.Parallel()

	var (
		totalProcessed int64
		testData       = generateTestData(20)
		mut            sync.Mutex
		collected      = make([]interface{}, 0, len(testData))
	)

	bat := batcher.New(func(datas ...interface{}) error {
		mut.Lock()
		collected = append(collected, datas...)
		mut.Unlock()
		atomic.AddInt64(&totalProcessed, int64(len(datas)))

		return nil
	},
		batcher.WithMaxBatchSize(10),
		batcher.WithMaxWaitInterval(300*time.Millisecond),
		batcher.WithWorkersCount(3),
		batcher.WithLogrusLogger(logrus.StandardLogger()),
	)

	err := bat.Insert(testData...)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = bat.Stop()
	require.NoError(t, err)

	processed := atomic.LoadInt64(&totalProcessed)
	expectedProcessed := int64(len(testData))
	require.Equal(t, expectedProcessed, processed)

	require.ElementsMatch(t, testData, collected, "testdata [%v]\n collected [%v]", testData, collected)
}

func Test_BatchStopNegative(t *testing.T) {
	t.Parallel()

	bat := batcher.New(func(datas ...interface{}) error {
		return nil
	},
		batcher.WithMaxBatchSize(10),
		batcher.WithMaxWaitInterval(300*time.Millisecond),
		batcher.WithWorkersCount(3),
		batcher.WithLogrusLogger(logrus.StandardLogger()),
	)

	err := bat.Stop()
	require.NoError(t, err)

	require.EqualError(t, bat.Insert("should not be inserted"), batcher.ErrStopped.Error())
}

func Test_BatchStopConcurrent(t *testing.T) {
	t.Parallel()

	bat := batcher.New(func(datas ...interface{}) error {
		return nil
	},
		batcher.WithMaxBatchSize(10),
		batcher.WithMaxWaitInterval(300*time.Millisecond),
		batcher.WithWorkersCount(3),
		batcher.WithLogrusLogger(logrus.StandardLogger()),
	)

	go func() {
		for {
			err := bat.Insert(generateTestData(5)...)
			if errors.Is(err, batcher.ErrStopped) {
				break
			}
		}
	}()

	time.Sleep(time.Millisecond * 100)

	err := bat.Stop()
	require.NoError(t, err)
}

func Test_BatchGenerics(t *testing.T) {
	t.Parallel()

	t.Run("interface{}", func(t *testing.T) {
		t.Parallel()

		bat := batcher.New(func(datas ...interface{}) error {
			return nil
		},
			batcher.WithMaxBatchSize(10),
			batcher.WithMaxWaitInterval(300*time.Millisecond),
			batcher.WithLogrusLogger(logrus.StandardLogger()),
		)

		err := bat.Insert(generateTestData(20)...)
		require.NoError(t, err)
		err = bat.Stop()
		require.NoError(t, err)
	})

	t.Run("int", func(t *testing.T) {
		t.Parallel()

		bat := batcher.New(func(datas ...int) error {
			return nil
		},
			batcher.WithMaxBatchSize(10),
			batcher.WithMaxWaitInterval(300*time.Millisecond),
			batcher.WithLogrusLogger(logrus.StandardLogger()),
		)

		err := bat.Insert([]int{1, 2, 3, 4, 5, 6, 7}...)
		require.NoError(t, err)
		err = bat.Stop()
		require.NoError(t, err)
	})

	t.Run("string", func(t *testing.T) {
		t.Parallel()

		bat := batcher.New(func(datas ...string) error {
			return nil
		},
			batcher.WithMaxBatchSize(10),
			batcher.WithMaxWaitInterval(300*time.Millisecond),
			batcher.WithLogrusLogger(logrus.StandardLogger()),
		)

		err := bat.Insert([]string{"one", "two"}...)
		require.NoError(t, err)
		err = bat.Stop()
		require.NoError(t, err)
	})

	t.Run("struct", func(t *testing.T) {
		t.Parallel()

		type TestStruct struct {
			TestField string
		}

		bat := batcher.New(func(datas ...TestStruct) error {
			return nil
		},
			batcher.WithMaxBatchSize(10),
			batcher.WithMaxWaitInterval(300*time.Millisecond),
			batcher.WithLogrusLogger(logrus.StandardLogger()),
		)

		err := bat.Insert([]TestStruct{{"one"}, {"two"}}...)
		require.NoError(t, err)
		err = bat.Stop()
		require.NoError(t, err)
	})
}
