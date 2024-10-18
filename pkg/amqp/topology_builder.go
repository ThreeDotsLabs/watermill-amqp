package amqp

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyBuilder is responsible for declaring exchange, queues and queues binding.
//
// Default TopologyBuilder is DefaultTopologyBuilder.
// If you need a custom-built topology, you should implement your own TopologyBuilder and pass it to the amqp.Config:
//
//	config := NewDurablePubSubConfig()
//	config.TopologyBuilder = MyProCustomBuilder{}
type TopologyBuilder interface {
	BuildTopology(channel *amqp.Channel, params BuildTopologyParams, config Config, logger watermill.LoggerAdapter) error
	ExchangeDeclare(channel *amqp.Channel, exchangeName string, config Config) error
}

// BuildTopologyParams are parameters for building AMQP topology.
type BuildTopologyParams struct {
	Topic        string
	QueueName    string
	ExchangeName string
	RoutingKey   string
}

type DefaultTopologyBuilder struct{}

func (builder *DefaultTopologyBuilder) ExchangeDeclare(channel *amqp.Channel, exchangeName string, config Config) error {
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

func (builder *DefaultTopologyBuilder) BuildTopology(channel *amqp.Channel, params BuildTopologyParams, config Config, logger watermill.LoggerAdapter) error {
	if _, err := channel.QueueDeclare(
		params.QueueName,
		config.Queue.Durable,
		config.Queue.AutoDelete,
		config.Queue.Exclusive,
		config.Queue.NoWait,
		config.Queue.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot declare queue")
	}

	logger.Debug("Queue declared", nil)

	if params.ExchangeName == "" {
		logger.Debug("No exchange to declare", nil)
		return nil
	}
	if err := builder.ExchangeDeclare(channel, params.ExchangeName, config); err != nil {
		return errors.Wrap(err, "cannot declare exchange")
	}

	logger.Debug("Exchange declared", nil)

	if err := channel.QueueBind(
		params.QueueName,
		params.RoutingKey,
		params.ExchangeName,
		config.QueueBind.NoWait,
		config.QueueBind.Arguments,
	); err != nil {
		return errors.Wrap(err, "cannot bind queue")
	}
	return nil
}
