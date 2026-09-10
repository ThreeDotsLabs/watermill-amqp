package amqp_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
	"github.com/stretchr/testify/require"
)

func TestConnectionConcurrentStateAccess(t *testing.T) {
	for _, action := range []string{"reconnect", "close"} {
		t.Run(action, func(t *testing.T) {
			config := amqp.NewDurableQueueConfig(amqpURI())
			conn, err := amqp.NewConnection(config.Connection, watermill.NopLogger{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })

			stop := make(chan struct{})
			done := make(chan struct{})
			started := make(chan struct{})
			go func() {
				defer close(done)
				close(started)
				for {
					select {
					case <-stop:
						return
					default:
						_ = conn.IsConnected()
						_ = conn.Connected()
						_ = conn.Connection()
						runtime.Gosched()
					}
				}
			}()
			t.Cleanup(func() {
				close(stop)
				<-done
			})
			<-started

			if action == "close" {
				require.NoError(t, conn.Close())
				require.Eventually(t, func() bool { return !conn.IsConnected() }, 10*time.Second, time.Millisecond)
				return
			}

			for i := 0; i < 3; i++ {
				previous := conn.Connection()
				require.NoError(t, previous.Close())
				require.Eventually(t, func() bool {
					return conn.IsConnected() && conn.Connection() != previous
				}, 10*time.Second, time.Millisecond)

				// The replacement connection must be usable, not just marked connected.
				channel, err := conn.Connection().Channel()
				require.NoError(t, err)
				require.NoError(t, channel.Close())
			}
		})
	}
}
