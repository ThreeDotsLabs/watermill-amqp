package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	stdAmqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCorrelatingMarshaler(t *testing.T) {
	marshaler := amqp.CorrelatingMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)
	assert.Equal(t, marshaled.CorrelationId, msg.UUID)
	assert.Equal(t, marshaled.DeliveryMode, stdAmqp.Persistent)

	unmarshaledMsg, err := marshaler.Unmarshal(publishingToDelivery(marshaled))
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}

func TestCorrelatingMarshaler_without_correlation_id(t *testing.T) {
	marshaler := amqp.CorrelatingMarshaler{}

	msg := message.NewMessage("", nil)
	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)
	assert.Empty(t, marshaled.CorrelationId)

	unmarshaledMsg, err := marshaler.Unmarshal(publishingToDelivery(marshaled))
	require.NoError(t, err)

	assert.Empty(t, unmarshaledMsg.UUID)
}

func TestCorrelatingMarshaler_not_persistent(t *testing.T) {
	marshaler := amqp.CorrelatingMarshaler{NotPersistentDeliveryMode: true}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	assert.EqualValues(t, marshaled.DeliveryMode, 0)
}

func TestCorrelatingMarshaler_postprocess_publishing(t *testing.T) {
	marshaler := amqp.CorrelatingMarshaler{
		PostprocessPublishing: func(publishing stdAmqp.Publishing) stdAmqp.Publishing {
			publishing.ContentType = "application/json"

			return publishing
		},
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	assert.Equal(t, marshaled.ContentType, "application/json")
}

func BenchmarkCorrelatingMarshaler_Marshal(b *testing.B) {
	m := amqp.CorrelatingMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	var err error

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = m.Marshal(msg)
	}
	b.StopTimer()

	assert.NoError(b, err)
}

func BenchmarkCorrelatingMarshaler_Unmarshal(b *testing.B) {
	m := amqp.CorrelatingMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}

	consumedMsg := publishingToDelivery(marshaled)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = m.Unmarshal(consumedMsg)
	}
	b.StopTimer()

	assert.NoError(b, err)
}
