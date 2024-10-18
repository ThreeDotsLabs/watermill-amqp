//go:build stress
// +build stress

package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func TestPublishSubscribe_pubsub_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      true,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createPubSub,
		createPubSubWithConsumerGroup,
	)
}

func TestPublishSubscribe_queue_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      false,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createQueuePubSub,
		nil,
	)
}

func TestPublishSubscribe_transactional_stress(t *testing.T) {
	tests.TestPubSubStressTest(
		t,
		tests.Features{
			ConsumerGroups:                      false,
			ExactlyOnceDelivery:                 false,
			GuaranteedOrder:                     true,
			GuaranteedOrderWithSingleSubscriber: true,
			Persistent:                          true,
		},
		createTransactionalPubSub,
		nil,
	)
}
