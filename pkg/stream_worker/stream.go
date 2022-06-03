package stream_worker

import (
	kinesisStreamer "datastream/pkg/kinesis_streamer"
	"fmt"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	"time"
)

var (
	ErrRecordSizeExceeded = errors.New("Data must be less than or equal to 1MB in size")
	Channel               = make(chan *kinesis.PutRecordsRequestEntry, 20000)
)

const (
	batchMaxLength       = 250
	defaultFlushInterval = 2 * time.Second
	MaxRecordSize        = 1 << 20 // 1MiB
	maxRequestSize       = 5 << 20 // 5MiB
)

func ListenToPayWorkerChannel() {
	batchSize := 0
	jsonBatch := make([]*kinesis.PutRecordsRequestEntry, 0, batchMaxLength)
	ticker := time.NewTicker(defaultFlushInterval)

	flush := func(msg string) {
		ticker.Stop()
		//go kinesisStreamer.flushStream(jsonBatch, msg)
		kinesisStreamer.FlushStream(kinesisStreamer.PayWorkerStreamName, jsonBatch, msg)
		jsonBatch = nil
		batchSize = 0
		ticker = time.NewTicker(defaultFlushInterval)
	}

	batchAppend := func(record *kinesis.PutRecordsRequestEntry) {
		recordSize := len(record.Data) + len([]byte(*record.PartitionKey))
		if batchSize+recordSize > maxRequestSize {
			flush("batch size")
		}

		batchSize += recordSize
		jsonBatch = append(jsonBatch, record)
		if len(jsonBatch) >= batchMaxLength {
			flush("batch length")
		}
	}

	for {
		select {
		case record := <-Channel:
			batchAppend(record)
		case <-ticker.C:
			if batchSize > 0 {
				flush("interval")
			}
		default:
			fmt.Println("Channel vazio, esperando registros do PayWorker")
			time.Sleep(200 * time.Millisecond)
		}
	}
}
