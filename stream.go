package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"strconv"
	"time"
)

var (
	ErrRecordSizeExceeded = errors.New("Data must be less than or equal to 1MB in size")
)

func listenToPayWorkerChannel() {
	batchSize := 0
	jsonBatch := make([]*kinesis.PutRecordsRequestEntry, 0, batchMaxLength)
	ticker := time.NewTicker(defaultFlushInterval)

	flush := func(msg string) {
		ticker.Stop()
		//go flushStream(jsonBatch, msg)
		flushStream(jsonBatch, msg)
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
		case record := <-channel:
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

func StreamPayments(records []*Payment) error {
	for _, payment := range records {
		paymentJson, err := json.Marshal(payment)
		if err != nil {
			fmt.Println(err)
			return err
		}

		if len(paymentJson) > maxRecordSize {
			return ErrRecordSizeExceeded
		}

		channel <- &kinesis.PutRecordsRequestEntry{
			Data:         []byte(string(paymentJson) + "\n"),
			PartitionKey: aws.String(strconv.Itoa(rand.Intn(100))),
		}
	}

	return nil
}

func flushStream(records []*kinesis.PutRecordsRequestEntry, reason string) {
	for {
		fmt.Printf("Enviando batch, reason: %s, length: %v\n", reason, len(records))
		putOutput, err := client.PutRecords(&kinesis.PutRecordsInput{
			Records:    records,
			StreamName: streamName,
		})

		if err != nil {
			fmt.Println("Falha no envio do batch")
			// dispatch failure to log? or DB?
			fmt.Println(err)
			return
		}

		failed := *putOutput.FailedRecordCount
		if failed == 0 {
			return
		}

		reason = "retry"
		records = filterFailures(records, putOutput.Records)
	}
}

func filterFailures(records []*kinesis.PutRecordsRequestEntry, response []*kinesis.PutRecordsResultEntry) (failedRecords []*kinesis.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			failedRecords = append(failedRecords, records[i])
		}
	}

	fmt.Printf("%v failed records, resending\n", len(failedRecords))
	return
}

func createAwsClient() *kinesis.Kinesis {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(producer.region),
		Endpoint:    aws.String(producer.endpoint),
		Credentials: credentials.NewStaticCredentials(producer.accessKeyID, producer.secretAccessKey, producer.sessionToken),
	})

	if err != nil {
		log.Panic(err)
	}

	return kinesis.New(s)
}

func describeStream(client *kinesis.Kinesis, streamName *string) {
	// verifica stream
	_, err := client.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})

	//fmt.Printf("%v\n", *stream)
	if err != nil {
		log.Panic(err)
	}
}
