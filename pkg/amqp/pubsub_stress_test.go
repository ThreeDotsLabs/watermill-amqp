// +build stress

package amqp_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

func TestPublishSubscribe_pubsub_stress(t *testing.T) {
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
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
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
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
	infrastructure.TestPubSubStressTest(
		t,
		infrastructure.Features{
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
