package amqp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Subscriber struct {
	*ConnectionWrapper

	config              Config
	closedChan          chan struct{}
	closeSubscriber     func() error
	subscriberWaitGroup *sync.WaitGroup
}

func NewSubscriber(config Config, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if err := config.ValidateSubscriber(); err != nil {
		return nil, err
	}

	conn, err := NewConnection(config.Connection, logger)
	if err != nil {
		return nil, err
	}

	var closed uint32
	closedChan := make(chan struct{})
	var subscriberWaitGroup sync.WaitGroup

	// Close the subscriber AND the connection when the subscriber is closed,
	// since this subscriber owns the connection.
	closeSubscriber := func() error {
		if !atomic.CompareAndSwapUint32(&closed, 0, 1) {
			// Already closed.
			return nil
		}

		logger.Debug("Closing subscriber.", nil)

		close(closedChan)

		subscriberWaitGroup.Wait()

		logger.Debug("Closing connection.", nil)

		return conn.Close()
	}

	return &Subscriber{
		conn,
		config,
		closedChan,
		closeSubscriber,
		&subscriberWaitGroup,
	}, nil
}

func NewSubscriberWithConnection(config Config, logger watermill.LoggerAdapter, conn *ConnectionWrapper) (*Subscriber, error) {
	if err := config.ValidateSubscriberWithConnection(); err != nil {
		return nil, err
	}

	var closed uint32
	closedChan := make(chan struct{})
	var subscriberWaitGroup sync.WaitGroup

	// Shared connections should not be closed by the subscriber. Just close the subscriber.
	closeSubscriber := func() error {
		if !atomic.CompareAndSwapUint32(&closed, 0, 1) {
			// Already closed.
			return nil
		}

		logger.Debug("Closing subscriber.", nil)

		close(closedChan)

		subscriberWaitGroup.Wait()

		return nil
	}

	return &Subscriber{
		conn,
		config,
		closedChan,
		closeSubscriber,
		&subscriberWaitGroup,
	}, nil
}

// Subscribe consumes messages from AMQP broker.
//
// Watermill's topic in Subscribe is not mapped to AMQP's topic, but depending on configuration, it can be mapped
// to exchange, queue or routing key.
// For detailed description of nomenclature mapping, please check "Nomenclature" paragraph in doc.go file.
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if s.Closed() {
		return nil, errors.New("pub/sub is closedChan")
	}

	if !s.IsConnected() {
		return nil, errors.New("not connected to AMQP")
	}

	logFields := watermill.LogFields{"topic": topic}

	out := make(chan *message.Message)

	queueName := s.config.Queue.GenerateName(topic)
	logFields["amqp_queue_name"] = queueName

	exchangeName := s.config.Exchange.GenerateName(topic)
	logFields["amqp_exchange_name"] = exchangeName

	routingKey := s.config.QueueBind.GenerateRoutingKey(topic)
	logFields["amqp_routing_key"] = routingKey

	if err := s.prepareConsume(topic, queueName, exchangeName, routingKey, logFields); err != nil {
		return nil, errors.Wrap(err, "failed to prepare consume")
	}

	s.subscriberWaitGroup.Add(1)
	s.connectionWaitGroup.Add(1)

	go func(ctx context.Context) {
		defer func() {
			close(out)
			s.logger.Info("Stopped consuming from AMQP channel", logFields)
			s.connectionWaitGroup.Done()
			s.subscriberWaitGroup.Done()
		}()

		reconnecting := false
	ReconnectLoop:
		for {
			s.logger.Debug("Waiting for s.connected or s.closing in ReconnectLoop", logFields)

			// to avoid race conditions with <-s.connected
			select {
			case <-s.closing:
				s.logger.Debug("Stopping ReconnectLoop (already closing)", logFields)
				break ReconnectLoop
			case <-s.closedChan:
				s.logger.Debug("Stopping ReconnectLoop (subscriber closing)", logFields)
				break ReconnectLoop
			default:
				// not closing yet
			}

			if reconnecting {
				if err := s.prepareConsume(topic, queueName, exchangeName, routingKey, logFields); err != nil {
					s.logger.Error("Failed to prepare consume", err, logFields)
				}
			}

			select {
			case <-s.connected:
				s.logger.Debug("Connection established in ReconnectLoop", logFields)
				// runSubscriber blocks until connection fails or Close() is called
				s.runSubscriber(ctx, out, queueName, logFields)
			case <-s.closing:
				s.logger.Debug("Stopping ReconnectLoop (closing)", logFields)
				break ReconnectLoop
			case <-ctx.Done():
				s.logger.Debug("Stopping ReconnectLoop (ctx done)", logFields)
				break ReconnectLoop
			}

			time.Sleep(time.Millisecond * 100)

			reconnecting = true
		}
	}(ctx)

	return out, nil
}

func (s *Subscriber) SubscribeInitialize(topic string) (err error) {
	if s.Closed() {
		return errors.New("pub/sub is closed")
	}

	if !s.IsConnected() {
		return errors.New("not connected to AMQP")
	}

	logFields := watermill.LogFields{"topic": topic}

	queueName := s.config.Queue.GenerateName(topic)
	logFields["amqp_queue_name"] = queueName

	exchangeName := s.config.Exchange.GenerateName(topic)
	logFields["amqp_exchange_name"] = exchangeName

	routingKey := s.config.QueueBind.GenerateRoutingKey(topic)
	logFields["amqp_routing_key"] = routingKey

	s.logger.Info("Initializing subscribe", logFields)

	return errors.Wrap(s.prepareConsume(topic, queueName, exchangeName, routingKey, logFields), "failed to prepare consume")
}

// Close closes all subscriptions with their output channels.
func (s *Subscriber) Close() error {
	return s.closeSubscriber()
}

func (s *Subscriber) prepareConsume(topic string, queueName string, routingKey string, exchangeName string, logFields watermill.LogFields) (err error) {
	channel, err := s.openSubscribeChannel(logFields)
	if err != nil {
		return err
	}
	defer func() {
		if channelCloseErr := channel.Close(); channelCloseErr != nil {
			err = multierror.Append(err, channelCloseErr)
		}
	}()

	params := BuildTopologyParams{
		Topic:        topic,
		QueueName:    queueName,
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
	}

	if err = s.config.TopologyBuilder.BuildTopology(channel, params, s.config, s.logger); err != nil {
		return err
	}

	s.logger.Debug("Queue bound to exchange", logFields)

	return nil
}

func (s *Subscriber) runSubscriber(
	ctx context.Context,
	out chan *message.Message,
	queueName string,
	logFields watermill.LogFields,
) {
	channel, err := s.openSubscribeChannel(logFields)
	if err != nil {
		s.logger.Error("Failed to open channel", err, logFields)
		return
	}
	defer func() {
		if err := channel.Close(); err != nil {
			s.logger.Error("Failed to close channel", err, logFields)
		}
	}()

	notifyCloseChannel := channel.NotifyClose(make(chan *amqp.Error, 1))

	sub := subscription{
		out:                out,
		logFields:          logFields,
		notifyCloseChannel: notifyCloseChannel,
		channel:            channel,
		queueName:          queueName,
		logger:             s.logger,
		closing:            s.closing,
		closedChan:         s.closedChan,
		config:             s.config,
	}

	s.logger.Info("Starting consuming from AMQP channel", logFields)

	sub.ProcessMessages(ctx)
}

func (s *Subscriber) openSubscribeChannel(logFields watermill.LogFields) (*amqp.Channel, error) {
	if !s.IsConnected() {
		return nil, errors.New("not connected to AMQP")
	}

	channel, err := s.amqpConnection.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "cannot open channel")
	}
	s.logger.Debug("Channel opened", logFields)

	if s.config.Consume.Qos != (QosConfig{}) {
		if err := channel.Qos(
			s.config.Consume.Qos.PrefetchCount,
			s.config.Consume.Qos.PrefetchSize,
			s.config.Consume.Qos.Global,
		); err != nil {
			return nil, errors.Wrap(err, "failed to set channel Qos")
		}
		s.logger.Debug("Qos set", logFields)
	}

	return channel, nil
}

type subscription struct {
	out                chan *message.Message
	logFields          watermill.LogFields
	notifyCloseChannel chan *amqp.Error
	channel            *amqp.Channel
	queueName          string

	logger     watermill.LoggerAdapter
	closing    chan struct{}
	closedChan chan struct{}
	config     Config
}

func (s *subscription) ProcessMessages(ctx context.Context) {
	amqpMsgs, err := s.createConsumer(s.queueName, s.channel)
	if err != nil {
		s.logger.Error("Failed to start consuming messages", err, s.logFields)
		return
	}

ConsumingLoop:
	for {
		select {
		case amqpMsg := <-amqpMsgs:
			if err := s.processMessage(ctx, amqpMsg, s.out, s.logFields); err != nil {
				s.logger.Error("Processing message failed, sending nack", err, s.logFields)

				if err := s.nackMsg(amqpMsg); err != nil {
					s.logger.Error("Cannot nack message", err, s.logFields)

					// something went really wrong when we cannot nack, let's reconnect
					break ConsumingLoop
				}
			}
			continue ConsumingLoop

		case <-s.notifyCloseChannel:
			s.logger.Error("Channel closed, stopping ProcessMessages", nil, s.logFields)
			break ConsumingLoop

		case <-s.closing:
			s.logger.Info("Closing from Subscriber received", s.logFields)
			break ConsumingLoop

		case <-s.closedChan:
			s.logger.Info("Subscriber closed", s.logFields)
			break ConsumingLoop

		case <-ctx.Done():
			s.logger.Info("Closing from ctx received", s.logFields)
			break ConsumingLoop
		}
	}
}

func (s *subscription) createConsumer(queueName string, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	amqpMsgs, err := channel.Consume(
		queueName,
		s.config.Consume.Consumer,
		false, // autoAck must be set to false - acks are managed by Watermill
		s.config.Consume.Exclusive,
		s.config.Consume.NoLocal,
		s.config.Consume.NoWait,
		s.config.Consume.Arguments,
	)
	if err != nil {
		return nil, errors.Wrap(err, "cannot consume from channel")
	}

	return amqpMsgs, nil
}

func (s *subscription) processMessage(
	ctx context.Context,
	amqpMsg amqp.Delivery,
	out chan *message.Message,
	logFields watermill.LogFields,
) error {
	msg, err := s.config.Marshaler.Unmarshal(amqpMsg)
	if err != nil {
		return err
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	msg.SetContext(ctx)
	defer cancelCtx()

	msgLogFields := logFields.Add(watermill.LogFields{"message_uuid": msg.UUID})
	s.logger.Trace("Unmarshaled message", msgLogFields)

	select {
	case <-s.closing:
		s.logger.Info("Message not consumed, pub/sub is closing", msgLogFields)
		return s.nackMsg(amqpMsg)
	case <-s.closedChan:
		s.logger.Info("Message not consumed, subscriber is closed", msgLogFields)
		return s.nackMsg(amqpMsg)
	case out <- msg:
		s.logger.Trace("Message sent to consumer", msgLogFields)
	}

	select {
	case <-s.closing:
		s.logger.Trace("Closing pub/sub, message discarded before ack", msgLogFields)
		return s.nackMsg(amqpMsg)
	case <-s.closedChan:
		s.logger.Info("Message not consumed, subscriber is closed", msgLogFields)
		return s.nackMsg(amqpMsg)
	case <-msg.Acked():
		s.logger.Trace("Message Acked", msgLogFields)
		return amqpMsg.Ack(false)
	case <-msg.Nacked():
		s.logger.Trace("Message Nacked", msgLogFields)
		return s.nackMsg(amqpMsg)
	}
}

func (s *subscription) nackMsg(amqpMsg amqp.Delivery) error {
	return amqpMsg.Nack(false, !s.config.Consume.NoRequeueOnNack)
}
