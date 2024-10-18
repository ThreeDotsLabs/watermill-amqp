package main

import (
	"fmt"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
)

func main() {
	for i := 0; i < 10; i++ {
		err := tryConnecting()
		if err == nil {
			os.Exit(0)
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Println("Failed to connect to AMQP")
	os.Exit(1)
}

func amqpURI() string {
	uri := os.Getenv("WATERMILL_TEST_AMQP_URI")
	if uri != "" {
		return uri
	}

	return "amqp://guest:guest@localhost:5672/"
}

func tryConnecting() error {
	logger := watermill.NewStdLogger(false, false)
	uri := amqpURI()

	_, err := amqp.NewPublisher(
		amqp.NewDurablePubSubConfig(uri, nil),
		logger,
	)
	if err != nil {
		return err
	}

	_, err = amqp.NewSubscriber(
		amqp.NewDurablePubSubConfig(uri, amqp.GenerateQueueNameTopicNameWithSuffix("wait-for")),
		logger,
	)
	if err != nil {
		return err
	}

	return nil
}
