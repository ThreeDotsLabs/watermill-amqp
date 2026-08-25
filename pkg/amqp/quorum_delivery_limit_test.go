package amqp_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	stdAmqp "github.com/rabbitmq/amqp091-go"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Quorum queues need RabbitMQ >= 3.8; default CI is still 3.7.
const minRabbitMQMajor, minRabbitMQMinor = 3, 8

const (
	testDeliveryLimit   = 3
	deliveryCountHeader = "x-delivery-count"
	// Must exceed the worst-case redelivery gap on a loaded CI runner, since silence is
	// how the read loop decides the broker has stopped redelivering.
	quietPeriod = 15 * time.Second
)

// TestQuorumQueueDeliveryLimit checks a forever-failing handler is dead-lettered at
// delivery-limit. On RabbitMQ 4.3 that only happens if failures use basic.reject.
func TestQuorumQueueDeliveryLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping delivery-limit integration test in short mode")
	}
	skipUnlessRabbitMQAtLeast(t, minRabbitMQMajor, minRabbitMQMinor)

	logger := watermill.NewStdLogger(true, false)

	topic := "test_delivery_limit_" + watermill.NewShortUUID()
	deadLetterExchange := topic + "_dlx"
	deadLetterQueue := topic + "_dlq"

	declareDeadLetterTopology(t, deadLetterExchange, deadLetterQueue)

	config := amqp.NewDurableQueueConfig(amqpURI())
	config.Queue.Arguments = stdAmqp.Table{
		"x-queue-type":           "quorum",
		"x-delivery-limit":       testDeliveryLimit,
		"x-dead-letter-exchange": deadLetterExchange,
	}
	// Quorum redeliveries add int counter headers; DefaultMarshaler requires strings.
	config.Marshaler = amqp.DefaultMarshaler{PreprocessDelivery: stringifyHeaders}

	subscriber, err := amqp.NewSubscriber(config, logger)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, subscriber.Close())
	}()

	publisher, err := amqp.NewPublisher(config, logger)
	require.NoError(t, err)

	// Generous: the read loop waits out quietPeriod and dead-lettering is polled after it.
	// Cancelling early closes the messages channel and fails the length assertion instead.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)
	deleteQueueOnCleanup(t, topic)

	require.NoError(t, publisher.Publish(topic, message.NewMessage(watermill.NewUUID(), []byte("poison"))))
	require.NoError(t, publisher.Close())

	// Cap reads so an unbounded redelivery loop fails the test instead of hanging.
	var deliveryCounts []string
	maxDeliveries := testDeliveryLimit + 5

ReadLoop:
	for len(deliveryCounts) < maxDeliveries {
		select {
		case msg, ok := <-messages:
			if !ok {
				break ReadLoop
			}

			deliveryCounts = append(deliveryCounts, msg.Metadata.Get(deliveryCountHeader))
			msg.Nack()
		case <-time.After(quietPeriod):
			break ReadLoop
		}
	}

	assert.Len(
		t,
		deliveryCounts,
		testDeliveryLimit+1,
		"expected the initial delivery plus %d redeliveries, saw %s values %v",
		testDeliveryLimit,
		deliveryCountHeader,
		deliveryCounts,
	)

	// EventuallyWithT, not Eventually: the condition runs off the test goroutine, where a
	// require failure would Goexit the poller and hide the real AMQP error.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		depth, err := queueDepth(deadLetterQueue)
		assert.NoError(c, err)
		assert.Equal(c, 1, depth)
	}, 20*time.Second, time.Second, "message was not dead-lettered once delivery-limit was reached")
}

// skipUnlessRabbitMQAtLeast uses Properties["version"], not Connection.Major/Minor (AMQP 0.9).
func skipUnlessRabbitMQAtLeast(t *testing.T, major, minor int) {
	t.Helper()

	connection, err := stdAmqp.Dial(amqpURI())
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, connection.Close())
	}()

	version, _ := connection.Properties["version"].(string)
	require.NotEmpty(t, version, "broker did not advertise a version property")

	atLeast, err := rabbitMQAtLeast(version, major, minor)
	require.NoErrorf(t, err, "refusing to skip the delivery-limit regression test on an unparseable broker version")

	if !atLeast {
		t.Skipf("requires RabbitMQ >= %d.%d (quorum queues), got %s", major, minor, version)
	}
}

// rabbitMQAtLeast errors rather than returning false on an unparseable version, so a
// version it cannot read fails the build instead of silently skipping the regression.
func rabbitMQAtLeast(version string, major, minor int) (bool, error) {
	var maj, min int
	if _, err := fmt.Sscanf(version, "%d.%d", &maj, &min); err != nil {
		return false, fmt.Errorf("parse RabbitMQ version %q: %w", version, err)
	}
	if maj != major {
		return maj > major, nil
	}
	return min >= minor, nil
}

func TestRabbitMQAtLeast(t *testing.T) {
	for version, expected := range map[string]bool{
		"3.8.0":  true,
		"3.9.1":  true,
		"4.3.0":  true,
		"3.7.28": false,
		"2.0.0":  false,
	} {
		atLeast, err := rabbitMQAtLeast(version, 3, 8)
		require.NoError(t, err, version)
		assert.Equal(t, expected, atLeast, version)
	}

	for _, version := range []string{"not-a-version", "4", ""} {
		_, err := rabbitMQAtLeast(version, 3, 8)
		assert.Error(t, err, version)
	}
}

func stringifyHeaders(delivery stdAmqp.Delivery) stdAmqp.Delivery {
	for key, value := range delivery.Headers {
		if _, ok := value.(string); !ok {
			delivery.Headers[key] = fmt.Sprintf("%v", value)
		}
	}

	return delivery
}

func declareDeadLetterTopology(t *testing.T, exchange, queue string) {
	t.Helper()

	channel, closeChannel := amqpChannel(t)
	defer closeChannel()

	require.NoError(t, channel.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil))

	_, err := channel.QueueDeclare(queue, true, false, false, false, stdAmqp.Table{
		"x-queue-type": "quorum",
	})
	require.NoError(t, err)

	require.NoError(t, channel.QueueBind(queue, "", exchange, false, nil))

	deleteQueueOnCleanup(t, queue)

	t.Cleanup(func() {
		cleanupChannel, closeCleanupChannel := amqpChannel(t)
		defer closeCleanupChannel()

		assert.NoError(t, cleanupChannel.ExchangeDelete(exchange, false, false))
	})
}

// queueDepth takes no *testing.T so it is safe to call from a polling goroutine.
func queueDepth(queue string) (int, error) {
	connection, err := stdAmqp.Dial(amqpURI())
	if err != nil {
		return 0, err
	}
	defer func() { _ = connection.Close() }()

	channel, err := connection.Channel()
	if err != nil {
		return 0, err
	}
	defer func() { _ = channel.Close() }()

	q, err := channel.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}

	return q.Messages, nil
}

func deleteQueueOnCleanup(t *testing.T, queue string) {
	t.Helper()

	t.Cleanup(func() {
		channel, closeChannel := amqpChannel(t)
		defer closeChannel()

		_, err := channel.QueueDelete(queue, false, false, false)
		assert.NoError(t, err)
	})
}

func amqpChannel(t *testing.T) (*stdAmqp.Channel, func()) {
	t.Helper()

	connection, err := stdAmqp.Dial(amqpURI())
	require.NoError(t, err)

	channel, err := connection.Channel()
	require.NoError(t, err)

	return channel, func() {
		assert.NoError(t, channel.Close())
		assert.NoError(t, connection.Close())
	}
}
