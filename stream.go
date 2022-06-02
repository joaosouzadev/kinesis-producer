package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"log"
	"math/rand"
	"strconv"
)

func resetJsonBatch() {
	jsonBatch = nil
}

func prepareAndSendStream() {
	mutex.Lock()
	defer mutex.Unlock()

	records := make([]*kinesis.PutRecordsRequestEntry, len(jsonBatch))

	for k, rec := range jsonBatch {
		records[k] = &kinesis.PutRecordsRequestEntry{
			Data:         []byte(rec),
			PartitionKey: aws.String(strconv.Itoa(rand.Intn(100))),
			//PartitionKey: aws.String("ledger"),
		}
	}

	flushStream(records)
}

func flushStream(records []*kinesis.PutRecordsRequestEntry) {
	if len(records) == 0 {
		fmt.Println("Nenhum record para enviar, retornando")
		return
	}

	fmt.Printf("Sending %v data to Kinesis\n", len(records))
	putOutput, err := client.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
		StreamName: streamName,
	})

	if err != nil {
		fmt.Println("Falha no envio do batch")
		fmt.Println(err)
		return
	}

	if *putOutput.FailedRecordCount > 0 {
		for i := 0; i < len(putOutput.Records); i++ {
			if putOutput.Records[i].ErrorCode != nil {
				fmt.Printf("JSON nao enviado, erro: %v, reenviando JSON para channel de envio\n", *putOutput.Records[i].ErrorCode)
				//fmt.Printf("%v\n", string(records[i].Data))
				channel <- string(records[i].Data) + "\n" // ou criar vetor com json falhos e chamar flushStream recursivamente?
			}
			fmt.Println("[", len(channel), "]")
		}
		fmt.Printf("%v records falharam\n", *putOutput.FailedRecordCount)
	} else {
		fmt.Println("Batch successfully sent to Kinesis")
	}

	resetJsonBatch()
	//fmt.Printf("%v\n", *putOutput)
}

func createAwsClient() *kinesis.Kinesis {
	// connect to aws
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
