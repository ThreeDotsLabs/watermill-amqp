// +build reconnect

package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func TestPublishSubscribe_reconnect(t *testing.T) {
	infrastructure.TestReconnect(
		t,
		infrastructure.TestContext{
			TestID: infrastructure.NewTestID(),
			Features: infrastructure.Features{
				ConsumerGroups:                      true,
				ExactlyOnceDelivery:                 false,
				GuaranteedOrder:                     true,
				GuaranteedOrderWithSingleSubscriber: true,
				Persistent:                          true,
				RestartServiceCommand:               []string{"docker", "restart", "project_rabbitmq_1"},
			},
		},
		createTransactionalPubSub,
	)
}
