package amqp_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
)

// When the topology cannot be prepared after the consumer stopped (here the queue is locked
// by another connection), the subscriber must back off instead of retrying every 100 ms,
// and Close must not wait for the backoff to finish.
func TestSubscriber_ResubscribeBacksOff(t *testing.T) {
	t.Parallel()

	topic := "resubscribe_backoff_" + watermill.NewShortUUID()
	config := amqp.NewDurableQueueConfig(amqpURI())
	logger := &countingLogger{msg: "Failed to prepare consume"}

	subscriber, err := amqp.NewSubscriber(config, logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	messages, err := subscriber.Subscribe(ctx, topic)
	require.NoError(t, err)
	go func() {
		for msg := range messages {
			msg.Ack()
		}
	}()

	lockQueue(t, config.Queue.GenerateName(topic))

	time.Sleep(3 * time.Second)
	attempts := logger.count.Load()
	assert.Positive(t, attempts, "the subscriber never tried to prepare the topology again")
	assert.LessOrEqual(t, attempts, int64(6), "the subscriber retried without backing off")

	closed := make(chan error, 1)
	go func() { closed <- subscriber.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited for the resubscribe backoff")
	}
}

// lockQueue replaces the queue with an exclusive one owned by another connection,
// so every declare and consume from the subscriber fails with RESOURCE_LOCKED.
func lockQueue(t *testing.T, name string) {
	t.Helper()

	conn, err := amqp091.Dial(amqpURI())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDelete(name, false, false, false)
	require.NoError(t, err)

	_, err = ch.QueueDeclare(name, false, true, true, false, nil)
	require.NoError(t, err)
}

type countingLogger struct {
	msg   string
	count atomic.Int64
}

func (l *countingLogger) Error(msg string, _ error, _ watermill.LogFields) {
	if msg == l.msg {
		l.count.Add(1)
	}
}

func (l *countingLogger) Info(string, watermill.LogFields)  {}
func (l *countingLogger) Debug(string, watermill.LogFields) {}
func (l *countingLogger) Trace(string, watermill.LogFields) {}

func (l *countingLogger) With(watermill.LogFields) watermill.LoggerAdapter { return l }
