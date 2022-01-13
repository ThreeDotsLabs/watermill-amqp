package amqp

import (
	"fmt"
	"sync/atomic"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type channel interface {
	AMQPChannel() *amqp.Channel
	Close() error
}

type channelProvider interface {
	Channel() (channel, error)
	CloseChannel(c channel) error
	Close()
}

func newChannelProvider(conn *ConnectionWrapper, poolSize int, logger watermill.LoggerAdapter) (channelProvider, error) {
	if poolSize == 0 {
		return &defaultChannelProvider{conn}, nil
	}

	return newPooledChannelProvider(conn, poolSize, logger)
}

type pooledChannel struct {
	logger     watermill.LoggerAdapter
	conn       *ConnectionWrapper
	amqpChan   *amqp.Channel
	closedChan chan *amqp.Error
}

func newPooledChannel(conn *ConnectionWrapper, logger watermill.LoggerAdapter) (*pooledChannel, error) {
	c := &pooledChannel{
		logger,
		conn,
		nil,
		nil,
	}

	if err := c.openAMQPChannel(); err != nil {
		return nil, fmt.Errorf("open AMQP channel: %w", err)
	}

	return c, nil
}

func (c *pooledChannel) AMQPChannel() *amqp.Channel {
	return c.amqpChan
}

func (c *pooledChannel) openAMQPChannel() error {
	var err error

	c.amqpChan, err = c.conn.amqpConnection.Channel()
	if err != nil {
		return fmt.Errorf("create AMQP channel: %w", err)
	}

	c.closedChan = make(chan *amqp.Error, 1)

	c.amqpChan.NotifyClose(c.closedChan)

	return nil
}

func (c *pooledChannel) validate() error {
	select {
	case e := <-c.closedChan:
		c.logger.Info("AMQP channel was closed. Opening new channel.", watermill.LogFields{"close-error": e.Error()})

		return c.openAMQPChannel()
	default:
		return nil
	}
}

func (c *pooledChannel) Close() error {
	return c.amqpChan.Close()
}

type channelWrapper struct {
	*amqp.Channel
}

func (c *channelWrapper) AMQPChannel() *amqp.Channel {
	return c.Channel
}

// defaultChannelProvider simply opens a new channel when Channel() is called and closes the channel
// when CloseChannel is called.
type defaultChannelProvider struct {
	conn *ConnectionWrapper
}

func (p *defaultChannelProvider) Channel() (channel, error) {
	amqpChan, err := p.conn.amqpConnection.Channel()
	if err != nil {
		return nil, fmt.Errorf("create AMQP channel: %w", err)
	}

	return &channelWrapper{Channel: amqpChan}, nil
}

func (p *defaultChannelProvider) CloseChannel(c channel) error {
	return c.Close()
}

func (p *defaultChannelProvider) Close() {
	// Nothing to do.
}

// pooledChannelProvider maintains a pool of channels which are opened immediately upon creation of the provider.
// The Channel() function returns an existing channel from the pool. If no channel is available then the caller must
// wait until a channel is returned to the pool (with the CloseChannel function). Channels in the pool are closed when
// this provider's Close() function is called.
// This provider improves performance in high volume systems and also acts as a throttle to prevent the AMQP server from
// overloading.
type pooledChannelProvider struct {
	logger     watermill.LoggerAdapter
	conn       *ConnectionWrapper
	channels   []*pooledChannel
	closed     uint32
	chanPool   chan *pooledChannel
	closedChan chan struct{}
}

func newPooledChannelProvider(conn *ConnectionWrapper, poolSize int, logger watermill.LoggerAdapter) (channelProvider, error) {
	logger.Info("Creating pooled channel provider", watermill.LogFields{"pool-size": poolSize})

	channels := make([]*pooledChannel, poolSize)

	chanPool := make(chan *pooledChannel, poolSize)

	// Create the channels and add them to the pool.

	for i := 0; i < poolSize; i++ {
		c, err := newPooledChannel(conn, logger)
		if err != nil {
			return nil, err
		}

		channels[i] = c

		chanPool <- c
	}

	return &pooledChannelProvider{
		logger,
		conn,
		channels,
		0,
		chanPool,
		make(chan struct{}),
	}, nil
}

func (p *pooledChannelProvider) Channel() (channel, error) {
	if p.isClosed() {
		return nil, errors.New("channel pool is closed")
	}

	select {
	case c := <-p.chanPool:
		// Ensure that the existing AMQP channel is still open.
		if err := c.validate(); err != nil {
			return nil, err
		}

		return c, nil

	case <-p.closedChan:
		return nil, errors.New("provider is closed")
	}
}

func (p *pooledChannelProvider) CloseChannel(c channel) error {
	if p.isClosed() {
		return nil
	}

	pc, ok := c.(*pooledChannel)
	if !ok {
		return errors.New("channel must be of type pooledChannel")
	}

	p.chanPool <- pc

	return nil
}

func (p *pooledChannelProvider) Close() {
	if !atomic.CompareAndSwapUint32(&p.closed, 0, 1) {
		// Already closed.
		return
	}

	close(p.closedChan)

	p.logger.Info("Closing all channels in the pool", watermill.LogFields{"pool-size": len(p.channels)})

	for _, c := range p.channels {
		if err := c.Close(); err != nil {
			p.logger.Error("Error closing channel: %s", err, watermill.LogFields{})
		}
	}
}

func (p *pooledChannelProvider) isClosed() bool {
	return atomic.LoadUint32(&p.closed) != 0
}
