//go:build reconnect
// +build reconnect

package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe_reconnect(t *testing.T) {
	tests.TestReconnect(
		t,
		tests.TestContext{
			TestID: tests.NewTestID(),
			Features: tests.Features{
				ConsumerGroups:                      true,
				ExactlyOnceDelivery:                 false,
				GuaranteedOrder:                     true,
				GuaranteedOrderWithSingleSubscriber: true,
				Persistent:                          true,
				RestartServiceCommand:               []string{"docker-compose", "restart", "rabbitmq"},
			},
		},
		createTransactionalPubSub,
	)
}
