# RabbitMQ course: code and written lessons

Companion code for the **From Dev to Dev** RabbitMQ video course: a course built on real failures. Every episode teaches **one concept**, starts from one concrete problem and can be understood on its own. Each episode has a small Java project you can run, and a README that is the written version of the lesson.

> **Learning-project disclaimer:** the code is illustrative, not production-ready. No real email is sent anywhere: the email provider is simulated by the projects. The videos use an AI-generated (synthetic) narration.

## Episodes

| # | Episode | Concept |
|---|---|---|
| 1 | [Why a message broker? Stop making checkout wait](episodes/01-rabbitmq-why-a-broker/README.md) | A broker lets the caller answer now and leave slow work for a consumer to do later. |
| 2 | [RabbitMQ queues: the buffer between producer and consumer](episodes/02-rabbitmq-queues/README.md) | A queue is a buffer: it holds messages until a consumer takes them. |

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
cd episodes/02-rabbitmq-queues
./run.sh up            # start RabbitMQ in Docker and declare the queue
./run.sh experiment    # the experiment of the episode (see its README)
./run.sh counters      # print the queue's counters
./run.sh test          # unit tests (no broker needed)
./run.sh down          # stop RabbitMQ and delete its data
```

`./run.sh help` lists every command of that episode.

Every episode uses the same local ports (5672 and 15672), so run `./run.sh down` in one episode's folder before starting the next one. The management page is at <http://localhost:15672> (user `guest`, password `guest`, for local use only).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
