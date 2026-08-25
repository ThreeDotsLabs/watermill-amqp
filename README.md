# Watermill AMQP Pub/Sub
<img align="right" width="200" src="https://watermill.io/img/gopher.svg">

[![CI Status](https://github.com/ThreeDotsLabs/watermill-amqp/actions/workflows/master.yml/badge.svg)](https://github.com/ThreeDotsLabs/watermill-amqp/actions/workflows/master.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ThreeDotsLabs/watermill-amqp)](https://goreportcard.com/report/github.com/ThreeDotsLabs/watermill-amqp)

This is Pub/Sub for the [Watermill](https://watermill.io/) project.


See [DEVELOPMENT.md](./DEVELOPMENT.md) for more information about running and testing.

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

All Pub/Sub implementations can be found at [https://watermill.io/pubsubs/](https://watermill.io/pubsubs/).

Documentation: https://watermill.io/

Getting started guide: https://watermill.io/docs/getting-started/

Issues: https://github.com/ThreeDotsLabs/watermill/issues

## Quorum queues and delivery limits

Failed deliveries are returned to the broker with `basic.reject`, not `basic.nack`. On RabbitMQ
4.3 the two are not equivalent: only `basic.reject` increments a quorum queue's `x-delivery-count`,
so `x-delivery-limit` and dead-lettering do not work if a consumer nacks.

If you use quorum queues, note that a handler which keeps nacking now exhausts the delivery limit
instead of retrying forever. RabbitMQ 4.0+ applies a default `x-delivery-limit` of 20, so **without
a dead-letter exchange the message is dropped after 20 attempts**. Configure
`x-dead-letter-exchange` on queues whose handlers can fail transiently.

Shutdown paths still use `basic.nack`, which leaves `x-delivery-count` untouched, so restarting a
subscriber does not spend a message's retry budget.

## Contributing

All contributions are very much welcome. If you'd like to help with Watermill development,
please see [open issues](https://github.com/ThreeDotsLabs/watermill/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+)
and submit your pull request via GitHub.

## Support

If you didn't find the answer to your question in [the documentation](https://watermill.io/), feel free to ask us directly!

Please join us on the `#watermill` channel on the [Three Dots Labs Discord](https://discord.gg/QV6VFg4YQE).

## License

[MIT License](./LICENSE)
