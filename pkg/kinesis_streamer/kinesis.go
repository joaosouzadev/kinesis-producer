package kinesis_streamer

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"log"
	"os"
	"sync"
)

var (
	kinesisClient *kinesis.Kinesis
	err           error
	lock          = &sync.Mutex{}
)

const (
	PayWorkerStreamName string = "jv-akkad-test"
)

type KinesisConfig struct {
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

func LoadKinesisConfig() *KinesisConfig {
	return &KinesisConfig{
		region:          os.Getenv("KINESIS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
}

func NewKinesisStreamer() (*kinesis.Kinesis, error) {
	config := LoadKinesisConfig()
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.region),
		Endpoint:    aws.String(config.endpoint),
		Credentials: credentials.NewStaticCredentials(config.accessKeyID, config.secretAccessKey, config.sessionToken),
	})

	if err != nil {
		return nil, err
	}

	return kinesis.New(s), nil
}

func GetKinesisClient() *kinesis.Kinesis {
	if kinesisClient == nil {
		lock.Lock()
		defer lock.Unlock()
		if kinesisClient == nil {
			fmt.Println("Creating Kinesis Client instance!")
			kinesisClient, err = NewKinesisStreamer()
			if err != nil {
				log.Panic(err)
			}
		}
	}

	return kinesisClient
}

func FlushStream(streamName string, records []*kinesis.PutRecordsRequestEntry, reason string) {
	for {
		fmt.Printf("Enviando batch, reason: %s, length: %v\n", reason, len(records))
		putOutput, err := GetKinesisClient().PutRecords(&kinesis.PutRecordsInput{
			Records:    records,
			StreamName: aws.String(streamName),
		})

		if err != nil {
			fmt.Println("Falha no envio do batch")
			fmt.Println(err)
			// dispatch failure to log? or DB?
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

func describeStream(streamName *string) {
	// verifica stream
	_, err := GetKinesisClient().DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})

	//fmt.Printf("%v\n", *stream)
	if err != nil {
		log.Panic(err)
	}
}
