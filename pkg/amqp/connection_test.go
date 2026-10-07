package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

// TestPublishSubscribe_after_connection_drops covers the path a broker restart
// or a network failure takes: the connection closes while a subscriber is
// consuming, ConnectionWrapper reconnects, and publishers and subscribers
// carry on. Closing the connection from the client side takes that same path
// without restarting the broker, so the test runs alongside the others, race
// detector included.
//
// Each drop races the reconnect against every publisher and subscriber reading
// the connection state, and races the closed deliveries channel against the
// channel close notification, so the test drops the connection several times,
// each with a subscription of its own.
func TestPublishSubscribe_after_connection_drops(t *testing.T) {
	const drops = 5

	logger := watermill.NewStdLogger(false, false)
	config := amqp.NewDurableQueueConfig(amqpURI())

	conn, err := amqp.NewConnection(config.Connection, logger)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	publisher, subscriber := createQueuePubSubWithSharedConnection(t, config, conn, logger)
	t.Cleanup(func() {
		assert.NoError(t, publisher.Close())
		assert.NoError(t, subscriber.Close())
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	for range drops {
		topic := "reconnect_" + watermill.NewUUID()
		messages, err := subscriber.Subscribe(ctx, topic)
		require.NoError(t, err)

		// Receiving a message proves the subscription is consuming, so the
		// drop below closes its deliveries channel under it.
		warmUp := message.NewMessage(watermill.NewUUID(), []byte("before the drop"))
		require.NoError(t, publisher.Publish(topic, warmUp))
		ackReceived(t, messages)

		require.NoError(t, conn.Connection().Close())

		sent := message.NewMessage(watermill.NewUUID(), []byte("after the drop"))

		// Publishing fails until the connection is back.
		require.Eventually(t, func() bool {
			return publisher.Publish(topic, sent) == nil
		}, 30*time.Second, 50*time.Millisecond, "publisher never reconnected")

		// The warm-up ack may still have been in flight when the connection
		// closed; the broker then rightly redelivers it first.
		received := ackReceived(t, messages)
		if received.UUID == warmUp.UUID {
			received = ackReceived(t, messages)
		}
		assert.Equal(t, sent.UUID, received.UUID)
	}
}

func ackReceived(t *testing.T, messages <-chan *message.Message) *message.Message {
	t.Helper()

	select {
	case received := <-messages:
		received.Ack()
		return received
	case <-time.After(30 * time.Second):
		t.Fatal("no message received")
		return nil
	}
}
