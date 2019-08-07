package amqp_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func amqpURI() string {
	uri := os.Getenv("WATERMILL_TEST_AMQP_URI")
	if uri != "" {
		return uri
	}

	return "amqp://guest:guest@localhost:5672/"
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	publisher, err := amqp.NewPublisher(
		amqp.NewDurablePubSubConfig(
			amqpURI(),
			nil,
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		amqp.NewDurablePubSubConfig(
			amqpURI(),
			amqp.GenerateQueueNameTopicNameWithSuffix("test"),
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	publisher, err := amqp.NewPublisher(
		amqp.NewDurablePubSubConfig(
			amqpURI(),
			nil,
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		amqp.NewDurablePubSubConfig(
			amqpURI(),
			amqp.GenerateQueueNameTopicNameWithSuffix(consumerGroup),
		),
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func createTransactionalPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	config := amqp.NewDurablePubSubConfig(
		amqpURI(),
		amqp.GenerateQueueNameTopicNameWithSuffix("test"),
	)
	config.Publish.Transactional = true

	publisher, err := amqp.NewPublisher(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func TestPublishSubscribe_pubsub(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func createQueuePubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	config := amqp.NewDurableQueueConfig(
		amqpURI(),
	)

	publisher, err := amqp.NewPublisher(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriber(
		config,
		watermill.NewStdLogger(true, true),
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func TestPublishSubscribe_queue(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      false,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createQueuePubSub,
		nil,
	)
}

func TestPublishSubscribe_transactional_publish(t *testing.T) {
	tests.TestPublishSubscribe(
		t,
		tests.TestContext{
			TestID: tests.NewTestID(),
			Features: tests.Features{
				ConsumerGroups:                      true,
				ExactlyOnceDelivery:                 false,
				GuaranteedOrder:                     true,
				GuaranteedOrderWithSingleSubscriber: true,
				Persistent:                          true,
			},
		},
		createTransactionalPubSub,
	)
}
