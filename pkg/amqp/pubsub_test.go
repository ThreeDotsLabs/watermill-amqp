package amqp_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
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
	publisherCfg := amqp.NewDurablePubSubConfig(
		amqpURI(),
		nil,
	)

	return createPubSubWithConfig(t, publisherCfg)
}

func createPubSubWithDeliveryConfirmation(t *testing.T) (message.Publisher, message.Subscriber) {
	publisherCfg := amqp.NewDurablePubSubConfig(
		amqpURI(),
		nil,
	)

	publisherCfg.Publish.ConfirmDelivery = true

	return createPubSubWithConfig(t, publisherCfg)
}

func createPubSubWithPublisherChannelPool(t *testing.T) (message.Publisher, message.Subscriber) {
	publisherCfg := amqp.NewDurablePubSubConfig(
		amqpURI(),
		nil,
	)

	publisherCfg.Publish.ChannelPoolSize = 50

	return createPubSubWithConfig(t, publisherCfg)
}

func createPubSubWithPublisherChannelPoolAndDeliveryConfirmation(t *testing.T) (message.Publisher, message.Subscriber) {
	publisherCfg := amqp.NewDurablePubSubConfig(
		amqpURI(),
		nil,
	)

	publisherCfg.Publish.ChannelPoolSize = 50
	publisherCfg.Publish.ConfirmDelivery = true

	return createPubSubWithConfig(t, publisherCfg)
}

func createPubSubWithConfig(t *testing.T, publisherCfg amqp.Config) (message.Publisher, message.Subscriber) {
	publisher, err := amqp.NewPublisher(
		publisherCfg,
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

func TestPublishSubscribe_pubsub_delivery_confirmation(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSubWithDeliveryConfirmation,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_pubsub_with_channel_pool(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSubWithPublisherChannelPool,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_pubsub_with_channel_pool_and_delivery_confirmation(t *testing.T) {
	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSubWithPublisherChannelPoolAndDeliveryConfirmation,
		createPubSubWithConsumerGroup,
	)
}

func createQueuePubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	config := amqp.NewDurableQueueConfig(
		amqpURI(),
	)

	config.Publish.ChannelPoolSize = 25

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

func createQueuePubSubWithSharedConnection(t *testing.T, config amqp.Config, conn *amqp.ConnectionWrapper, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber) {
	t.Logf("Creating publisher/subscriber with shared connection")

	publisher, err := amqp.NewPublisherWithConnection(config, logger, conn)
	require.NoError(t, err)

	subscriber, err := amqp.NewSubscriberWithConnection(config, logger, conn)
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

func TestPublishSubscribe_queue_with_shared_connection(t *testing.T) {
	config := amqp.NewDurableQueueConfig(
		amqpURI(),
	)

	logger := watermill.NewStdLogger(true, true)

	conn, err := amqp.NewConnection(config.Connection, logger)
	require.NoError(t, err)

	tests.TestPubSub(
		t,
		tests.Features{
			ConsumerGroups:                      false,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			return createQueuePubSubWithSharedConnection(t, config, conn, logger)
		},
		nil,
	)
}

func TestSharedConnection(t *testing.T) {
	const topic = "topicXXX"

	config := amqp.Config{
		// No Connection field here
		Marshaler: amqp.DefaultMarshaler{},

		Exchange: amqp.ExchangeConfig{
			GenerateName: func(topic string) string {
				return ""
			},
		},
		Queue: amqp.QueueConfig{
			GenerateName: amqp.GenerateQueueNameTopicName,
			Durable:      true,
		},
		QueueBind: amqp.QueueBindConfig{
			GenerateRoutingKey: func(topic string) string {
				return ""
			},
		},
		Publish: amqp.PublishConfig{
			GenerateRoutingKey: func(topic string) string {
				return topic
			},
		},
		Consume: amqp.ConsumeConfig{
			Qos: amqp.QosConfig{
				PrefetchCount: 1,
			},
		},
		TopologyBuilder: &amqp.DefaultTopologyBuilder{},
	}

	logger := watermill.NewStdLogger(true, true)

	connConfig := amqp.ConnectionConfig{
		AmqpURI: amqpURI(),
	}

	conn, err := amqp.NewConnection(connConfig, logger)
	require.NoError(t, err)

	s, err := amqp.NewSubscriberWithConnection(config, logger, conn)
	require.NoError(t, err)

	msgChan, err := s.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	p, err := amqp.NewPublisherWithConnection(config, logger, conn)
	require.NoError(t, err)

	require.NoError(t, p.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("payload"))))

	select {
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message")
	case msg := <-msgChan:
		msg.Ack()
	}

	require.NoError(t, conn.Close())

	// After closing the connection, the subscriber message channel should also be closed.

	select {
	case _, open := <-msgChan:
		require.False(t, open)
	default:
		t.Error("messages channel is not closed")
	}
}

//func TestClose(t *testing.T) {
//	t.Parallel()
//
//	amqpConfig := amqp.NewDurablePubSubConfig(amqpURI(), func(topic string) string {
//		return "local-" + topic
//	})
//
//	s, err := amqp.NewSubscriber(amqpConfig, watermill.NewStdLogger(true, true))
//	require.NoError(t, err)
//
//	pub, err := amqp.NewPublisher(amqpConfig, watermill.NewStdLogger(true, true))
//
//	// todo
//	topicName := "test"
//
//	go func() {
//		msgs, err := s.Subscribe(context.Background(), topicName)
//		require.NoError(t, err)
//
//		for m := range msgs {
//			log.Println(m)
//		}
//	}()
//
//	time.Sleep(time.Second)
//
//	go tests.AddSimpleMessagesParallel(t, 50, pub, topicName, 50)
//
//	time.Sleep(time.Second * 5)
//
//	err = s.Close()
//	require.NoError(t, err)
//}
