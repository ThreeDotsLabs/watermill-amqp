package amqp

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// CorrelatingMarshaler will pass UUID through the AMQP native correlation ID rather than as a header
type CorrelatingMarshaler struct {
	// PostprocessPublishing can be used to make some extra processing with amqp.Publishing,
	// for example add CorrelationId and ContentType:
	//
	//  amqp.DefaultMarshaler{
	//		PostprocessPublishing: func(publishing stdAmqp.Publishing) stdAmqp.Publishing {
	//			publishing.CorrelationId = "correlation"
	//			publishing.ContentType = "application/json"
	//
	//			return publishing
	//		},
	//	}
	PostprocessPublishing func(amqp.Publishing) amqp.Publishing

	// When true, DeliveryMode will be not set to Persistent.
	//
	// DeliveryMode Transient means higher throughput, but messages will not be
	// restored on broker restart. The delivery mode of publishings is unrelated
	// to the durability of the queues they reside on. Transient messages will
	// not be restored to durable queues, persistent messages will be restored to
	// durable queues and lost on non-durable queues during server restart.
	NotPersistentDeliveryMode bool
}

func (cm CorrelatingMarshaler) Marshal(msg *message.Message) (amqp.Publishing, error) {
	headers := make(amqp.Table, len(msg.Metadata))

	for key, value := range msg.Metadata {
		headers[key] = value
	}

	publishing := amqp.Publishing{
		Body:          msg.Payload,
		Headers:       headers,
		CorrelationId: msg.UUID,
	}
	if !cm.NotPersistentDeliveryMode {
		publishing.DeliveryMode = amqp.Persistent
	}

	if cm.PostprocessPublishing != nil {
		publishing = cm.PostprocessPublishing(publishing)
	}

	return publishing, nil
}

func (cm CorrelatingMarshaler) Unmarshal(amqpMsg amqp.Delivery) (*message.Message, error) {
	msg := message.NewMessage(amqpMsg.CorrelationId, amqpMsg.Body)
	msg.Metadata = make(message.Metadata)

	for key, value := range amqpMsg.Headers {
		var ok bool
		msg.Metadata[key], ok = value.(string)
		if !ok {
			return nil, errors.Errorf("metadata %s is not a string, but %#v", key, value)
		}
	}

	return msg, nil
}
