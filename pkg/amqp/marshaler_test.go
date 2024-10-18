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

func TestDefaultMarshaler(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	_, headerExists := marshaled.Headers[amqp.DefaultMessageUUIDHeaderKey]
	assert.True(t, headerExists, "header %s doesn't exist", amqp.DefaultMessageUUIDHeaderKey)

	unmarshaledMsg, err := marshaler.Unmarshal(publishingToDelivery(marshaled))
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
	assert.Equal(t, marshaled.DeliveryMode, stdAmqp.Persistent)
}

func TestDefaultMarshaler_without_message_uuid(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), nil)
	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	delete(marshaled.Headers, amqp.DefaultMessageUUIDHeaderKey)

	unmarshaledMsg, err := marshaler.Unmarshal(publishingToDelivery(marshaled))
	require.NoError(t, err)

	assert.Empty(t, unmarshaledMsg.UUID)
}

func TestDefaultMarshaler_configured_message_uuid_header(t *testing.T) {
	headerKey := "custom_msg_uuid"
	marshaler := amqp.DefaultMarshaler{MessageUUIDHeaderKey: headerKey}

	msg := message.NewMessage(watermill.NewUUID(), nil)
	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	_, headerExists := marshaled.Headers[headerKey]
	assert.True(t, headerExists, "header %s doesn't exist", headerKey)

	unmarshaledMsg, err := marshaler.Unmarshal(publishingToDelivery(marshaled))
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, unmarshaledMsg.UUID)
}

func TestDefaultMarshaler_not_persistent(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{NotPersistentDeliveryMode: true}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	assert.EqualValues(t, marshaled.DeliveryMode, 0)
}

func TestDefaultMarshaler_postprocess_publishing(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{
		PostprocessPublishing: func(publishing stdAmqp.Publishing) stdAmqp.Publishing {
			publishing.CorrelationId = "correlation"
			publishing.ContentType = "application/json"

			return publishing
		},
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := marshaler.Marshal(msg)
	require.NoError(t, err)

	assert.Equal(t, marshaled.CorrelationId, "correlation")
	assert.Equal(t, marshaled.ContentType, "application/json")
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := amqp.DefaultMarshaler{}

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

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := amqp.DefaultMarshaler{}

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

func publishingToDelivery(marshaled stdAmqp.Publishing) stdAmqp.Delivery {
	return stdAmqp.Delivery{
		Body:          marshaled.Body,
		Headers:       marshaled.Headers,
		CorrelationId: marshaled.CorrelationId,
	}
}
