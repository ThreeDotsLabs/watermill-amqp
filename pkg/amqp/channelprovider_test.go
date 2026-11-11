package amqp

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/require"
)

// tcpProxy forwards TCP connections to the broker and can be stopped and
// restarted on the same address to deterministically simulate an outage.
type tcpProxy struct {
	target string
	addr   string

	mu       sync.Mutex
	listener net.Listener
	conns    []net.Conn
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()

	p := &tcpProxy{target: target}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p.addr = listener.Addr().String()
	p.start(listener)

	t.Cleanup(p.Stop)

	return p
}

func (p *tcpProxy) start(listener net.Listener) {
	p.mu.Lock()
	p.listener = listener
	p.mu.Unlock()

	go func() {
		for {
			clientConn, err := listener.Accept()
			if err != nil {
				return
			}

			targetConn, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = clientConn.Close()
				continue
			}

			p.mu.Lock()
			p.conns = append(p.conns, clientConn, targetConn)
			p.mu.Unlock()

			go func() { _, _ = io.Copy(targetConn, clientConn) }()
			go func() { _, _ = io.Copy(clientConn, targetConn) }()
		}
	}()
}

// Stop makes the broker unreachable: no new connections and all existing ones dropped.
func (p *tcpProxy) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}

	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// Restart brings the broker back on the same address.
func (p *tcpProxy) Restart(t *testing.T) {
	t.Helper()

	listener, err := net.Listen("tcp", p.addr)
	require.NoError(t, err)

	p.start(listener)
}

// TestPooledChannelProvider_DoesNotLeakSlotsOnValidateFailure reproduces a pool
// slot leak: when validate() failed (broker unreachable while reopening a dead
// channel), Channel() dropped the pooled channel without returning it to the
// pool. Each failure permanently shrank the pool, and once poolSize failures
// accumulated, every subsequent Channel() call blocked forever.
func TestPooledChannelProvider_DoesNotLeakSlotsOnValidateFailure(t *testing.T) {
	proxy := newTCPProxy(t, "localhost:5672")

	conn, err := NewConnection(ConnectionConfig{
		AmqpURI: fmt.Sprintf("amqp://guest:guest@%s/", proxy.addr),
		Reconnect: &ReconnectConfig{
			BackoffInitialInterval:     50 * time.Millisecond,
			BackoffRandomizationFactor: 0.2,
			BackoffMultiplier:          1.1,
			BackoffMaxInterval:         200 * time.Millisecond,
		},
	}, watermill.NopLogger{})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	const poolSize = 2
	provider, err := newChannelProvider(conn, poolSize, false, watermill.NopLogger{})
	require.NoError(t, err)
	defer provider.Close()

	// Sanity check: a channel can be obtained and returned while the broker is up.
	c, err := provider.Channel()
	require.NoError(t, err)
	require.NoError(t, provider.CloseChannel(c))

	// Kill the broker connection; the pooled channels die and every reopen
	// attempt fails until the proxy is restarted.
	proxy.Stop()

	require.Eventually(t, func() bool { return !conn.IsConnected() }, 5*time.Second, 10*time.Millisecond,
		"connection should be detected as lost")

	// Request channels more times than the pool has slots. Every call must
	// return an error promptly. Before the fix, each failure leaked a slot and
	// call poolSize+1 blocked forever on the empty pool.
	for i := 0; i < poolSize*2+1; i++ {
		result := make(chan error, 1)
		go func() {
			_, err := provider.Channel()
			result <- err
		}()

		select {
		case err := <-result:
			require.Error(t, err, "Channel() should fail while the broker is unreachable")
		case <-time.After(5 * time.Second):
			t.Fatalf("Channel() call %d blocked: pool slot leaked on validate failure", i+1)
		}
	}

	// Broker comes back; the pool must recover all slots.
	proxy.Restart(t)

	require.Eventually(t, func() bool { return conn.IsConnected() }, 10*time.Second, 10*time.Millisecond,
		"connection should be re-established")

	for i := 0; i < poolSize; i++ {
		var recovered channel
		require.Eventually(t, func() bool {
			c, err := provider.Channel()
			if err != nil {
				return false
			}
			recovered = c
			return true
		}, 10*time.Second, 50*time.Millisecond, "pooled channel %d should be usable again", i+1)
		require.NoError(t, recovered.AMQPChannel().ExchangeDeclare(
			"test_pool_recovery", "fanout", false, true, false, false, nil))
		require.NoError(t, provider.CloseChannel(recovered))
	}
}
