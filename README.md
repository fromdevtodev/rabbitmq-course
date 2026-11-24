# RabbitMQ course: code and written lessons

Companion code for the **From Dev to Dev** RabbitMQ video course: a course built on real failures. Every episode teaches **one concept**, starts from one concrete problem and can be understood on its own. Each episode has a small Java project you can run, and a README that is the written version of the lesson.

> **Learning-project disclaimer:** the code is illustrative, not production-ready. No real email is sent anywhere: the email provider is simulated by the projects. The videos use an AI-generated (synthetic) narration.

## Episodes

| # | Episode | Concept |
|---|---|---|
| 1 | [Why a message broker? Stop making checkout wait](episodes/01-rabbitmq-why-a-broker/README.md) | A broker lets the caller answer now and leave slow work for a consumer to do later. |
| 2 | [RabbitMQ queues: the buffer between producer and consumer](episodes/02-rabbitmq-queues/README.md) | A queue is a buffer: it holds messages until a consumer takes them. |
| 3 | [Exchanges and bindings: how a message finds its queue](episodes/03-rabbitmq-exchanges-and-bindings/README.md) | A producer publishes to an exchange, and bindings decide which queues receive a copy. |
| 4 | [Direct exchange: exact-match routing, and the default exchange](episodes/04-rabbitmq-direct-exchange/README.md) | A direct exchange copies a message to every queue whose binding key exactly equals the routing key. |
| 5 | [Topic exchange: routing with patterns (`*` and `#`)](episodes/05-rabbitmq-topic-exchange/README.md) | A topic exchange matches routing keys against word patterns: star is exactly one word, hash is zero or more. |
| 6 | [Fanout exchange: broadcast to every queue](episodes/06-rabbitmq-fanout-exchange/README.md) | A fanout exchange ignores the routing key and copies every message to every queue bound to it. |
| 7 | [Unroutable messages: when RabbitMQ drops yours without an error](episodes/07-rabbitmq-unroutable-messages/README.md) | A message that matches no binding is dropped silently, unless you ask RabbitMQ to give it back. |
| 8 | [Manual acknowledgements: what the ack really means](episodes/08-rabbitmq-manual-ack/README.md) | A delivery stays unacknowledged until the consumer answers, and RabbitMQ redelivers it if the consumer dies first. |
| 9 | [Nack and requeue: how one bad message loops forever](episodes/09-rabbitmq-nack-and-requeue/README.md) | Requeue puts a rejected message straight back, so a message that always fails is delivered again and again. |
| 10 | [Prefetch: stop a slow consumer from hoarding messages](episodes/10-rabbitmq-prefetch/README.md) | Prefetch caps how many unacknowledged messages RabbitMQ gives a consumer at once. |
| 11 | [Durability: what survives a RabbitMQ restart](episodes/11-rabbitmq-durability/README.md) | Only a durable queue holding persistent messages can survive a broker restart, and even then not every message is guaranteed. |
| 12 | [Publisher confirms: did RabbitMQ really take it?](episodes/12-rabbitmq-publisher-confirms/README.md) | A publisher confirm tells the producer the broker has taken responsibility for a message, not that anyone processed it. |

More episodes are added to this repository as they are published. The course covers four blocks: **foundations** (broker, queues, exchanges, routing), **consuming and reliability** (acknowledgements, retries, dead letters), **correctness** (duplicates, idempotency, ordering) and **operations and decisions** (quorum queues, flow control, observability, RabbitMQ versus Kafka), and ends with a closing project that puts them together.

The introduction video has no code of its own: it shows what the course covers and how an episode works.

## What you need

- Java 21
- Maven
- Docker (with Docker Compose v2)

No previous RabbitMQ knowledge is needed; episode 1 starts from zero.

## Run an episode

Each episode folder is a self-contained Maven project with its own `docker-compose.yml` and a `run.sh` that has one command per step of the lesson:

```bash
cd episodes/12-rabbitmq-publisher-confirms
./run.sh up            # start RabbitMQ in Docker
./run.sh experiment1   # the first experiment of the episode (see its README)
./run.sh counters      # print the queue's counters
./run.sh test          # unit tests (no broker needed)
./run.sh down          # stop RabbitMQ and delete its data
```

The experiment commands differ from episode to episode (`experiment`, `experiment1`, `experiment2`, ...), and `./run.sh help` lists every command of that episode. Each episode's README explains what its experiments do and what to predict before running them.

Every episode uses the same local ports (5672 and 15672), so run `./run.sh down` in one episode's folder before starting the next one. The management page is at <http://localhost:15672> (user `guest`, password `guest`, for local use only).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)