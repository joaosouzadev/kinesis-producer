package main

import (
	payworker "datastream/pkg/pay_worker"
	streamworker "datastream/pkg/stream_worker"
	"fmt"
	"time"
)

func main() {
	go payworker.StartPayWorker()
	go streamworker.ListenToPayWorkerChannel()

	for {
		fmt.Println("Akkad main thread doing other work")
		time.Sleep(2 * time.Second)
	}
}
