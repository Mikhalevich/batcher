# batcher
Simple golang batch task executor

###### Documentation 
[![Go Reference](https://pkg.go.dev/badge/github.com/Mikhalevich/batcher.svg)](https://pkg.go.dev/github.com/Mikhalevich/batcher)

### example
```golang
package main

import (
	"log"
	"os"
	"time"

	"github.com/Mikhalevich/batcher"
)

func main() {
	//nolint:mnd
	bat := batcher.New("example", func(datas ...int) error {
		log.Printf("received batch data: %v\n", datas)

		return nil
	},
		batcher.WithMaxBatchSize(10),
		batcher.WithMaxWaitInterval(15*time.Second),
		batcher.WithWorkersCount(4),
	)

	for i := 0; ; i++ {
		if err := bat.Insert(i); err != nil {
			log.Printf("insert error: %v", err)

			os.Exit(1)
		}

		time.Sleep(time.Second)
	}
}
```


## License

Batcher is released under the
[MIT License](http://www.opensource.org/licenses/MIT).
