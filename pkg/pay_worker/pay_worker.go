package pay_worker

import (
	streamworker "datastream/pkg/stream_worker"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"math/rand"
	"strconv"
	"time"
)

func StartPayWorker() {
	for {
		fmt.Println("PayWorker running")
		records := make([]*Payment, 2089)
		mockPayments(records)

		go func() {
			err := streamPayments(records)
			if err != nil {
				// log err
			}
		}()

		time.Sleep(10 * time.Second)
	}
}

func streamPayments(records []*Payment) error {
	for _, payment := range records {
		paymentJson, err := json.Marshal(payment)
		if err != nil {
			fmt.Println(err)
			return err
		}

		if len(paymentJson) > streamworker.MaxRecordSize {
			return streamworker.ErrRecordSizeExceeded
		}

		streamworker.Channel <- &kinesis.PutRecordsRequestEntry{
			Data:         []byte(string(paymentJson) + "\n"),
			PartitionKey: aws.String(strconv.Itoa(rand.Intn(100))),
		}
	}

	return nil
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
