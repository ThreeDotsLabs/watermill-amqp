package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyBuilder is responsible for declaring exchange, queues and queues binding.
//
// Default TopologyBuilder is DefaultTopologyBuilder.
// If you need custom built topology, you should implement your own TopologyBuilder and pass it to the amqp.Config:
//
// 	config := NewDurablePubSubConfig()
// 	config.TopologyBuilder = MyProCustomBuilder{}
//
type TopologyBuilder interface {
	BuildTopology(channel *amqp.Channel, queue *amqp.Queue, exchangeName string, config Config, logger watermill.LoggerAdapter) error
	ExchangeDeclare(channel *amqp.Channel, exchangeName string, config Config) error
	QueueDeclare(channel *amqp.Channel, queueName string, config Config, logger watermill.LoggerAdapter) (amqp.Queue, error)
}

type DefaultTopologyBuilder struct{}

func (builder DefaultTopologyBuilder) ExchangeDeclare(channel *amqp.Channel, exchangeName string, config Config) error {
	return channel.ExchangeDeclare(
		exchangeName,
		config.Exchange.Type,
		config.Exchange.Durable,
		config.Exchange.AutoDeleted,
		config.Exchange.Internal,
		config.Exchange.NoWait,
		config.Exchange.Arguments,
	)
}

func (builder *DefaultTopologyBuilder) QueueDeclare(channel *amqp.Channel, queueName string, config Config, logger watermill.LoggerAdapter) (amqp.Queue, error) {
	return channel.QueueDeclare(
		queueName,
		config.Queue.Durable,
		config.Queue.AutoDelete,
		config.Queue.Exclusive,
		config.Queue.NoWait,
		config.Queue.Arguments,
	)
}

func (builder *DefaultTopologyBuilder) BuildTopology(channel *amqp.Channel, queue *amqp.Queue, exchangeName string, config Config, logger watermill.LoggerAdapter) error {
	if exchangeName == "" {
		logger.Debug("No exchange to declare", nil)
		return nil
	}
	if err := builder.ExchangeDeclare(channel, exchangeName, config); err != nil {
		return errors.Wrap(err, "cannot declare exchange")
	}

	logger.Debug("Exchange declared", nil)

	if err := channel.QueueBind(
		queue.Name,
		config.QueueBind.GenerateRoutingKey(queue.Name),
		exchangeName,
		config.QueueBind.NoWait,
		config.QueueBind.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot bind queue")
	}
	return nil
}
