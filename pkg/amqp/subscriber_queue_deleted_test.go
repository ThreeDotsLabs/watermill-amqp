package amqp_test

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// When the broker cancels the consumer (for example because the queue was deleted),
// amqp091 closes the deliveries channel while the AMQP channel stays open.
// The subscriber must not turn that closed channel into an empty message.
func TestSubscriber_QueueDeleted_DoesNotEmitEmptyMessage(t *testing.T) {
	t.Parallel()

	topic := "queue_deleted_" + watermill.NewShortUUID()
	config := amqp.NewDurableQueueConfig(amqpURI())

	publisher, err := amqp.NewPublisher(config, watermill.NewStdLogger(false, false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = publisher.Close() })

	subscriber, err := amqp.NewSubscriber(config, watermill.NewStdLogger(true, false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = subscriber.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)

	deleteQueue(t, config.Queue.GenerateName(topic))

	sent := message.NewMessage(watermill.NewUUID(), []byte("after delete"))
	require.Eventually(t, func() bool {
		select {
		case msg := <-messages:
			msg.Ack()
			require.NotEmpty(t, msg.UUID, "received an empty message after the queue was deleted")
			return msg.UUID == sent.UUID
		default:
		}
		_ = publisher.Publish(topic, sent)
		return false
	}, 10*time.Second, 200*time.Millisecond, "subscriber did not resume after the queue was recreated")
}

func deleteQueue(t *testing.T, name string) {
	t.Helper()

	conn, err := amqp091.Dial(amqpURI())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()

	_, err = ch.QueueDelete(name, false, false, false)
	require.NoError(t, err)
}
