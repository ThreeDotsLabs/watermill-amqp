package amqp_test

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
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

func TestDefaultMarshaler_encoding_non_string_values(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{}
	delivery := createDelivery(map[string]interface{}{
		"foo":     "bar",
		"x-death": []interface{}{"foo", 2, true, []interface{}{"inside"}},
		"int":     int64(3),
		"bool":    true,
	})
	unmarshaledMsg, err := marshaler.Unmarshal(delivery)
	require.NoError(t, err)

	for _, key := range []string{"x-death_gob", "int_gob", "bool_gob"} {
		assert.NotEmpty(t, unmarshaledMsg.Metadata.Get(key), "key %s not serialized")
	}

	assert.Empty(t, unmarshaledMsg.Metadata.Get("foo_gob"))
}

func TestDefaultMarshaler_encoded_data(t *testing.T) {
	marshaler := amqp.DefaultMarshaler{}

	delivery := createDelivery(map[string]interface{}{
		"foo":     "bar",
		"x-death": []interface{}{"foo", 2, true, []interface{}{"inside"}},
		"int":     int64(3),
		"bool":    true,
	})
	unmarshaledMsg, err := marshaler.Unmarshal(delivery)
	require.NoError(t, err)

	decodedHeaders := stdAmqp.Table{
		// key foo has string value so Unmarshal not going to encode it.
		"foo": "bar",
	}

	for _, k := range []string{"x-death_gob", "int_gob", "bool_gob"} {
		switch k {
		case "x-death_gob":
			var v []interface{}
			err := decodeValue(unmarshaledMsg.Metadata[k], &v)
			decodedHeaders["x-death"] = v
			assert.NoError(t, err)
		case "int_gob":
			var v int64
			err := decodeValue(unmarshaledMsg.Metadata[k], &v)
			decodedHeaders["int"] = v
			assert.NoError(t, err)
		case "bool_gob":
			var v bool
			err := decodeValue(unmarshaledMsg.Metadata[k], &v)
			decodedHeaders["bool"] = v
			assert.NoError(t, err)
		}
	}

	assert.Equal(t, delivery.Headers, decodedHeaders)
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

func BenchmarkDefaultMarshaler_Unmarshal_Gob(b *testing.B) {
	m := amqp.DefaultMarshaler{}

	delivery := createDelivery(map[string]interface{}{
		"foo":     "bar",
		"x-death": []interface{}{"foo", 2, true, []interface{}{"inside"}},
		"int":     int64(3),
		"bool":    true,
	})

	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = m.Unmarshal(delivery)
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

func createDelivery(headers map[string]interface{}) stdAmqp.Delivery {
	return stdAmqp.Delivery{
		Body:          []byte("payload"),
		Headers:       headers,
		CorrelationId: "correlation",
	}
}

func decodeValue(value string, decodeTo interface{}) error {
	buf := bytes.NewBuffer([]byte(value))
	dec := gob.NewDecoder(buf)
	err := dec.Decode(decodeTo)
	return err
}
