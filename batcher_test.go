package batcher

import (
	"errors"
	"fmt"
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

	assert.EqualError(t, b.Insert("should not be inserted"), ErrStopped.Error())
}

func Test_BatchStopConcurrent(t *testing.T) {
	b := New("test_stop_concurrent", func(datas ...interface{}) error {
		return nil
	},
		WithMaxBatchSize(10),
		WithMaxWaitInterval(300*time.Millisecond),
		WithWorkersCount(3),
		WithLogrusLogger(logrus.StandardLogger()),
	)

	go func() {
		for {
			err := b.Insert(generateTestData(5)...)
			if errors.Is(err, ErrStopped) {
				break
			}
		}
	}()

	time.Sleep(time.Millisecond * 100)

	err := b.Stop()
	assert.NoError(t, err)
}

func Test_BatchGenerics(t *testing.T) {
	t.Run("interface{}", func(t *testing.T) {
		b := New("test_generic_interface{}", func(datas ...interface{}) error {
			fmt.Printf("received interface{} data: %v\n", datas)
			return nil
		},
			WithMaxBatchSize(10),
			WithMaxWaitInterval(300*time.Millisecond),
			WithLogrusLogger(logrus.StandardLogger()),
		)

		err := b.Insert(generateTestData(20)...)
		assert.NoError(t, err)
		err = b.Stop()
		assert.NoError(t, err)
	})

	t.Run("int", func(t *testing.T) {
		b := New("test_generic_int", func(datas ...int) error {
			fmt.Printf("received int data: %v\n", datas)
			return nil
		},
			WithMaxBatchSize(10),
			WithMaxWaitInterval(300*time.Millisecond),
			WithLogrusLogger(logrus.StandardLogger()),
		)

		err := b.Insert([]int{1, 2, 3, 4, 5, 6, 7}...)
		assert.NoError(t, err)
		err = b.Stop()
		assert.NoError(t, err)
	})

	t.Run("string", func(t *testing.T) {
		b := New("test_generic_string", func(datas ...string) error {
			fmt.Printf("received string data: %v\n", datas)
			return nil
		},
			WithMaxBatchSize(10),
			WithMaxWaitInterval(300*time.Millisecond),
			WithLogrusLogger(logrus.StandardLogger()),
		)

		err := b.Insert([]string{"one", "two"}...)
		assert.NoError(t, err)
		err = b.Stop()
		assert.NoError(t, err)
	})

	t.Run("struct", func(t *testing.T) {
		type TestStruct struct {
			TestField string
		}

		b := New("test_generic_struct", func(datas ...TestStruct) error {
			fmt.Printf("received struct data: %v\n", datas)
			return nil
		},
			WithMaxBatchSize(10),
			WithMaxWaitInterval(300*time.Millisecond),
			WithLogrusLogger(logrus.StandardLogger()),
		)

		err := b.Insert([]TestStruct{{"one"}, {"two"}}...)
		assert.NoError(t, err)
		err = b.Stop()
		assert.NoError(t, err)
	})
}
