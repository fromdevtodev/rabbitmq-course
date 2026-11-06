# RabbitMQ Course

Code and demos for the **From Dev to Dev** video course on RabbitMQ. Every video teaches **one concept** in 10–15 minutes, and every episode has its own small project here so you can run what you see on screen.

## Requirements

- Java 21 or later and Maven
- Docker with Docker Compose (RabbitMQ runs in a container)
- `curl`

Each episode folder has its own `README.md` with the exact commands, a `docker-compose.yml` and the code shown in the video. Episodes are self-contained: you do not need to run the previous ones.

## Episodes

| # | Episode | Status |
|---|---|---|
| 01 | [Why a message broker? Stop making checkout wait](episodes/01-rabbitmq-why-a-broker/) | available |
| 02 | [RabbitMQ queues: the buffer between producer and consumer](episodes/02-rabbitmq-queues/) | available |
| 03 | [Exchanges and bindings: how a message finds its queue](episodes/03-rabbitmq-exchanges-and-bindings/) | available |
| 04 | [Direct exchange: exact-match routing, and the default exchange](episodes/04-rabbitmq-direct-exchange/) | available |
| 05 | [Topic exchange: routing with patterns (* and #)](episodes/05-rabbitmq-topic-exchange/) | available |
| 06 | [Fanout exchange: broadcast to every queue](episodes/06-rabbitmq-fanout-exchange/) | available |
| 07 | [Unroutable messages: when RabbitMQ drops yours without an error](episodes/07-rabbitmq-unroutable-messages/) | available |
| 08 | [Manual acknowledgements: what the ack really means](episodes/08-rabbitmq-manual-ack/) | available |
| 09 | [Nack and requeue: how one bad message loops forever](episodes/09-rabbitmq-nack-and-requeue/) | available |
| 10 | [Prefetch: stop a slow consumer from hoarding messages](episodes/10-rabbitmq-prefetch/) | available |
| 11 | [Durability: what survives a RabbitMQ restart](episodes/11-rabbitmq-durability/) | planned |
| 12 | [Publisher confirms: did RabbitMQ really take it?](episodes/12-rabbitmq-publisher-confirms/) | planned |
| 13 | [Dead-letter exchanges: where failed messages go](episodes/13-rabbitmq-dead-letter-exchange/) | planned |
| 14 | [Delayed retries with TTL and dead-lettering](episodes/14-rabbitmq-delayed-retry-ttl/) | planned |
| 15 | [Bounded retries: park the message and raise an alert](episodes/15-rabbitmq-bounded-retries-dlq/) | planned |
| 16 | [RabbitMQ: The ACK Gap That Duplicates Payments](episodes/16-rabbitmq-ack-gap/) | planned |
| 17 | [Idempotent consumers: make duplicates harmless](episodes/17-rabbitmq-idempotent-consumers/) | planned |
| 18 | [Message ordering in RabbitMQ: what is guaranteed and what is not](episodes/18-rabbitmq-message-ordering/) | planned |
| 19 | [Quorum queues: high availability that survives a node failure](episodes/19-rabbitmq-quorum-queues/) | planned |
| 20 | [Flow control: why RabbitMQ blocks your publishers](episodes/20-rabbitmq-flow-control/) | planned |
| 21 | [RabbitMQ observability: four numbers that find a stuck consumer](episodes/21-rabbitmq-observability/) | planned |
| 22 | [RabbitMQ vs Kafka: stop asking which is better](episodes/22-rabbitmq-vs-kafka/) | planned |

Extras:

| Episode | Status |
|---|---|
| Spring AMQP: producers, listeners and converters | planned |
| Spring AMQP error handling: why a failing listener loops | planned |
| RabbitMQ Streams: when a queue isn't enough | planned |
| Headers exchange: routing on message attributes | planned |

Folders appear here as each episode is released. Status values: `available` (video and code published) and `planned`.

## How an episode folder is organised

```text
episodes/NN-<episode-id>/
├── README.md          what the episode covers, how to run it, experiments
├── docker-compose.yml RabbitMQ (and anything else the episode needs)
├── pom.xml, src/      the code shown in the video
├── examples/          small snippets and sample messages
└── recordings/        scripts used to record the demos shown in the video (macOS)
```

## About the code

The projects are learning material, not production templates. Each episode README lists what it deliberately does not cover; later episodes cover reliability topics such as publisher confirms, bounded retries, dead-lettering and idempotency.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md). The project is released under the license in [LICENSE](LICENSE).
