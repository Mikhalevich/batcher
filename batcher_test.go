package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func generateTestData(count int) []interface{} {
	var testData []interface{}
	for i := 0; i < count; i++ {
		testData = append(testData, i)
	}
	return testData
}

func Test_BatchInsertOneByOne(t *testing.T) {
	var (
		totalProcessed int64
		testData       = generateTestData(20)
		mu             sync.Mutex
		collected      = make([]interface{}, 0, len(testData))
	)

	b := New("test_one_by_one", func(datas ...interface{}) error {
		mu.Lock()
		collected = append(collected, datas...)
		mu.Unlock()
		atomic.AddInt64(&totalProcessed, int64(len(datas)))

		return nil
	},
		WithMaxBatchSize(10),
		WithMaxWaitInterval(300*time.Millisecond),
		WithWorkersCount(3),
		WithLogrusLogger(logrus.StandardLogger()),
	)

	for _, d := range testData {
		err := b.Insert(d)
		assert.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	err := b.Stop()
	assert.NoError(t, err)

	processed := atomic.LoadInt64(&totalProcessed)
	expectedProcessed := int64(len(testData))
	assert.Equal(t, expectedProcessed, processed)

	assert.ElementsMatch(t, testData, collected, "testdata [%v]\n collected [%v]", testData, collected)
}

func Test_BatchInsertList(t *testing.T) {
	var (
		totalProcessed int64
		testData       = generateTestData(20)
		mu             sync.Mutex
		collected      = make([]interface{}, 0, len(testData))
	)

	b := New("test_insert_list", func(datas ...interface{}) error {
		mu.Lock()
		collected = append(collected, datas...)
		mu.Unlock()
		atomic.AddInt64(&totalProcessed, int64(len(datas)))

		return nil
	},
		WithMaxBatchSize(10),
		WithMaxWaitInterval(300*time.Millisecond),
		WithWorkersCount(3),
		WithLogrusLogger(logrus.StandardLogger()),
	)

	err := b.Insert(testData...)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = b.Stop()
	assert.NoError(t, err)

	processed := atomic.LoadInt64(&totalProcessed)
	expectedProcessed := int64(len(testData))
	assert.Equal(t, expectedProcessed, processed)

	assert.ElementsMatch(t, testData, collected, "testdata [%v]\n collected [%v]", testData, collected)
}

func Test_BatchStopNegative(t *testing.T) {
	b := New("test_stop_negative", func(datas ...interface{}) error {
		return nil
	},
		WithMaxBatchSize(10),
		WithMaxWaitInterval(300*time.Millisecond),
		WithWorkersCount(3),
		WithLogrusLogger(logrus.StandardLogger()),
	)

	err := b.Stop()
	assert.NoError(t, err)

	assert.EqualError(t, b.Insert("should not be inserted"), "batch closed")
}
