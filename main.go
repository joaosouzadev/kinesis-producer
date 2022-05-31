package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/joho/godotenv"
)

var (
	producer   AWSKinesis
	client     *kinesis.Kinesis
	streamName *string
	channel    chan string
	jsonBatch  []string
	mutex      sync.Mutex
)

const (
	batch_max_length int = 200
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

	channel = make(chan string, 50000)
	jsonBatch = make([]string, 0)
}

func main() {
	client = createAwsClient()
	streamName = aws.String(producer.stream)
	//describeStream(client, streamName)

	go listenToChannel()
	go payWorker()

	for {
		fmt.Println("main thread doing other work")
		time.Sleep(2 * time.Second)
	}
}

func listenToChannel() {
	ticker := time.NewTicker(4000 * time.Millisecond)

	go func() {
		for {
			select {
			case record := <-channel:
				jsonBatch = append(jsonBatch, record)

				if len(jsonBatch) == batch_max_length { // ultimo batch nunca vai enviar, colocar outro parametro (tempo?)
					fmt.Println("Enviando batch pois atingiu limite")
					prepareAndSendStream()
				}
			default:
				fmt.Println("Channel vazio, esperando pay worker")
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("Enviando batch automaticamente após 4sec")
				prepareAndSendStream()
			}
		}
	}()
}

func payWorker() {
	for {
		fmt.Println("PayWorker running")
		records := make([]*Payment, 5025)
		mockPayments(records)

		go func() {
			for _, payment := range records {
				paymentJson, err := json.Marshal(payment)
				if err != nil {
					fmt.Println(err)
					return
				}
				channel <- string(paymentJson) + "\n"
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
