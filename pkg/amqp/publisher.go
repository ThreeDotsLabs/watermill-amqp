package amqp

import (
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	*ConnectionWrapper

	config                  Config
	publishBindingsLock     sync.RWMutex
	publishBindingsPrepared map[string]struct{}
	closePublisher          func() error
	chanProvider            channelProvider
}

func NewPublisher(config Config, logger watermill.LoggerAdapter) (*Publisher, error) {
	if err := config.ValidatePublisher(); err != nil {
		return nil, err
	}

	var err error

	conn, err := NewConnection(config.Connection, logger)
	if err != nil {
		return nil, fmt.Errorf("create new connection: %w", err)
	}

	chanProvider, err := newChannelProvider(conn, config.Publish.ChannelPoolSize, config.Publish.ConfirmDelivery, logger)
	if err != nil {
		return nil, fmt.Errorf("create new channel pool: %w", err)
	}

	// Close the connection when the publisher is closed since this publisher owns the connection.
	closePublisher := func() error {
		logger.Debug("Closing publisher connection.", nil)

		chanProvider.Close()

		return conn.Close()
	}

	return &Publisher{
		conn,
		config,
		sync.RWMutex{},
		make(map[string]struct{}),
		closePublisher,
		chanProvider,
	}, nil
}

func NewPublisherWithConnection(config Config, logger watermill.LoggerAdapter, conn *ConnectionWrapper) (*Publisher, error) {
	if err := config.ValidatePublisher(); err != nil {
		return nil, err
	}

	chanProvider, err := newChannelProvider(conn, config.Publish.ChannelPoolSize, config.Publish.ConfirmDelivery, logger)
	if err != nil {
		return nil, fmt.Errorf("create new channel pool: %w", err)
	}

	// Shared connections should not be closed by the publisher.
	closePublisher := func() error {
		logger.Debug("Publisher closed.", nil)

		chanProvider.Close()

		return nil
	}

	return &Publisher{
		conn,
		config,
		sync.RWMutex{},
		make(map[string]struct{}),
		closePublisher,
		chanProvider,
	}, nil
}

// Publish publishes messages to AMQP broker.
// Publish is blocking until the broker has received and saved the message.
// Publish is always thread safe.
//
// Watermill's topic in Publish is not mapped to AMQP's topic, but depending on configuration it can be mapped
// to exchange, queue or routing key.
// For detailed description of nomenclature mapping, please check "Nomenclature" paragraph in doc.go file.
func (p *Publisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.Closed() {
		return errors.New("pub/sub is connection closedChan")
	}

	if !p.IsConnected() {
		return errors.New("not connected to AMQP")
	}

	p.connectionWaitGroup.Add(1)
	defer p.connectionWaitGroup.Done()

	c, err := p.chanProvider.Channel()
	if err != nil {
		return errors.Wrap(err, "cannot open channel")
	}
	defer func() {
		if channelCloseErr := p.chanProvider.CloseChannel(c); channelCloseErr != nil {
			err = multierror.Append(err, channelCloseErr)
		}
	}()

	channel := c.AMQPChannel()

	if p.config.Publish.Transactional {
		if err := p.beginTransaction(channel); err != nil {
			return err
		}

		defer func() {
			err = p.commitTransaction(channel, err)
		}()
	}

	if err := p.preparePublishBindings(topic, channel); err != nil {
		return err
	}

	logFields := make(watermill.LogFields, 3)

	exchangeName := p.config.Exchange.GenerateName(topic)
	logFields["amqp_exchange_name"] = exchangeName

	routingKey := p.config.Publish.GenerateRoutingKey(topic)
	logFields["amqp_routing_key"] = routingKey

	for _, msg := range messages {
		if err := p.publishMessage(exchangeName, routingKey, msg, c, logFields); err != nil {
			return err
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.closePublisher()
}

func (p *Publisher) beginTransaction(channel *amqp.Channel) error {
	if err := channel.Tx(); err != nil {
		return errors.Wrap(err, "cannot start transaction")
	}

	p.logger.Trace("Transaction begun", nil)

	return nil
}

func (p *Publisher) commitTransaction(channel *amqp.Channel, err error) error {
	if err != nil {
		if rollbackErr := channel.TxRollback(); rollbackErr != nil {
			return multierror.Append(err, rollbackErr)
		}
	}

	return channel.TxCommit()
}

func (p *Publisher) publishMessage(
	exchangeName, routingKey string,
	msg *message.Message,
	channel channel,
	logFields watermill.LogFields,
) error {
	logFields = logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})

	p.logger.Trace("Publishing message", logFields)

	amqpMsg, err := p.config.Marshaler.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "cannot marshal message")
	}

	if err = channel.AMQPChannel().Publish(
		exchangeName,
		routingKey,
		p.config.Publish.Mandatory,
		p.config.Publish.Immediate,
		amqpMsg,
	); err != nil {
		return errors.Wrap(err, "cannot publish msg")
	}

	if !channel.DeliveryConfirmationEnabled() {
		p.logger.Trace("Message published", logFields)

		return nil
	}

	p.logger.Trace("Message published. Waiting for delivery confirmation.", logFields)

	if !channel.Delivered() {
		return fmt.Errorf("delivery not confirmed for message [%s]", msg.UUID)
	}

	p.logger.Trace("Delivery confirmed for message", logFields)

	return nil
}

func (p *Publisher) preparePublishBindings(topic string, channel *amqp.Channel) error {
	p.publishBindingsLock.RLock()
	_, prepared := p.publishBindingsPrepared[topic]
	p.publishBindingsLock.RUnlock()

	if prepared {
		return nil
	}

	p.publishBindingsLock.Lock()
	defer p.publishBindingsLock.Unlock()

	if p.config.Exchange.GenerateName(topic) != "" {
		if err := p.config.TopologyBuilder.ExchangeDeclare(channel, p.config.Exchange.GenerateName(topic), p.config); err != nil {
			return err
		}
	}

	p.publishBindingsPrepared[topic] = struct{}{}

	return nil
}
