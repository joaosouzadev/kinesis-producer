package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"
)

var (
	producer   AWSKinesis
	client     *kinesis.Kinesis
	streamName *string
	channel    chan *kinesis.PutRecordsRequestEntry
)

const (
	batchMaxLength       int = 500
	defaultFlushInterval     = 2 * time.Second
	maxRecordSize            = 1 << 20 // 1MiB
	maxRequestSize           = 5 << 20 // 5MiB
)

func init() {
	e := godotenv.Load()
	if e != nil {
		fmt.Print(e)
	}
	producer = AWSKinesis{
		stream:          os.Getenv("KINESIS_STREAM_NAME"),
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}

	channel = make(chan *kinesis.PutRecordsRequestEntry, 20000)
}

func main() {
	client = createAwsClient()
	streamName = aws.String(producer.stream)
	//describeStream(client, streamName)

	go payWorker()

	for {
		fmt.Println("Akkad main thread doing other work")
		time.Sleep(2 * time.Second)
	}
}

func payWorker() {
	go listenToPayWorkerChannel()

	for {
		fmt.Println("PayWorker running")
		records := make([]*Payment, 5089)
		mockPayments(records)

		go func() {
			err := StreamPayments(records)
			if err != nil {
				// log err
			}
		}()

		time.Sleep(10 * time.Second)
	}
}

func mockPayments(records []*Payment) {
	for k := 0; k < cap(records); k++ {
		amount := rand.Intn(10000000)
		charge := Entry{Type: "charge", Amount: amount, Installment: 1, EffectiveDate: "2022-01-01"}
		chargeFee := Entry{Type: "charge_fee", Amount: 10, Installment: 1, EffectiveDate: "2022-01-01"}

		entries := []Entry{charge, chargeFee}
		payment := &Payment{MerchantId: strconv.Itoa(rand.Intn(10000)), PaymentId: strconv.Itoa(rand.Intn(10000)), Amount: amount, Entry: entries}

		records[k] = payment
	}
}
