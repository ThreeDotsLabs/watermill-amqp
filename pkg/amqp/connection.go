package amqp

import (
	"sync"
	"sync/atomic"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/cenkalti/backoff/v3"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionWrapper manages an AMQP connection.
type ConnectionWrapper struct {
	config ConnectionConfig

	logger watermill.LoggerAdapter

	amqpConnection     *amqp.Connection
	amqpConnectionLock sync.Mutex
	connected          chan struct{}

	closing chan struct{}
	closed  uint32

	connectionWaitGroup sync.WaitGroup
}

// NewConnection returns a new connection wrapper.
func NewConnection(
	config ConnectionConfig,
	logger watermill.LoggerAdapter,
) (*ConnectionWrapper, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	pubSub := &ConnectionWrapper{
		config:    config,
		logger:    logger,
		closing:   make(chan struct{}),
		connected: make(chan struct{}),
	}
	if err := pubSub.connect(); err != nil {
		return nil, err
	}

	go pubSub.handleConnectionClose()

	return pubSub, nil
}

func (c *ConnectionWrapper) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		// Already closed.
		return nil
	}

	close(c.closing)

	c.logger.Info("Closing AMQP Pub/Sub", nil)
	defer c.logger.Info("Closed AMQP Pub/Sub", nil)

	c.connectionWaitGroup.Wait()

	if err := c.amqpConnection.Close(); err != nil {
		c.logger.Error("Connection close error", err, nil)
	}

	return nil
}

func (c *ConnectionWrapper) connect() error {
	c.amqpConnectionLock.Lock()
	defer c.amqpConnectionLock.Unlock()

	amqpConfig := c.config.AmqpConfig
	if amqpConfig != nil && amqpConfig.TLSClientConfig != nil && c.config.TLSConfig != nil {
		return errors.New("both Config.AmqpConfig.TLSClientConfig and Config.TLSConfig are set")
	}

	var connection *amqp.Connection
	var err error

	if amqpConfig != nil {
		connection, err = amqp.DialConfig(c.config.AmqpURI, *c.config.AmqpConfig)
	} else if c.config.TLSConfig != nil {
		connection, err = amqp.DialTLS(c.config.AmqpURI, c.config.TLSConfig)
	} else {
		connection, err = amqp.Dial(c.config.AmqpURI)
	}

	if err != nil {
		return errors.Wrap(err, "cannot connect to AMQP")
	}
	c.amqpConnection = connection
	close(c.connected)

	c.logger.Info("Connected to AMQP", nil)

	return nil
}

func (c *ConnectionWrapper) Connection() *amqp.Connection {
	return c.amqpConnection
}

func (c *ConnectionWrapper) Connected() chan struct{} {
	return c.connected
}

func (c *ConnectionWrapper) IsConnected() bool {
	select {
	case <-c.connected:
		return true
	default:
		return false
	}
}

func (c *ConnectionWrapper) Closed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *ConnectionWrapper) handleConnectionClose() {
	for {
		c.logger.Debug("handleConnectionClose is waiting for c.connected", nil)
		<-c.connected
		c.logger.Debug("handleConnectionClose is for connection or Pub/Sub close", nil)

		notifyCloseConnection := c.amqpConnection.NotifyClose(make(chan *amqp.Error))

		select {
		case <-c.closing:
			c.logger.Debug("Stopping handleConnectionClose", nil)
			c.connected = make(chan struct{})
			return
		case err := <-notifyCloseConnection:
			c.connected = make(chan struct{})
			c.logger.Error("Received close notification from AMQP, reconnecting", err, nil)
			c.reconnect()
		}
	}
}

func (c *ConnectionWrapper) reconnect() {
	reconnectConfig := c.config.Reconnect
	if reconnectConfig == nil {
		reconnectConfig = DefaultReconnectConfig()
	}

	if err := backoff.Retry(func() error {
		err := c.connect()
		if err == nil {
			return nil
		}

		c.logger.Error("Cannot reconnect to AMQP, retrying", err, nil)

		if c.Closed() {
			return backoff.Permanent(errors.Wrap(err, "closing AMQP connection"))
		}

		return err
	}, reconnectConfig.backoffConfig()); err != nil {
		// should only exit, if closing Pub/Sub
		c.logger.Error("AMQP reconnect failed failed", err, nil)
	}
}
