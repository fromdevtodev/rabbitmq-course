# 09 — Nack and requeue: how one bad message loops forever

**Level:** Engineer  
**Episode:** Nack and requeue: how one bad message loops forever  
**Language:** English  
**Code:** Java 21 · Maven · RabbitMQ Java client · Docker Compose  
**Requires:** [Episode 08 — Manual acknowledgements](../08-rabbitmq-manual-ack/)

## YouTube

[Watch this episode]({{youtubeUrl}})

This folder is the companion project for the ninth episode of the course. The README is the **written version of the lesson**: together with the video it should leave the concept complete. Read it to review the ideas, then run the experiments yourself.

> **The one sentence of this episode**
>
> **Requeue puts a rejected message straight back, so a message that always fails is delivered again and again.**

> **Learning-project disclaimer:** the "email worker" sends no email: it waits a moment and then succeeds or fails, so that you can watch what RabbitMQ does with each answer. The two terminal demos in the video are **real recordings** of this project. The code is illustrative, not production-ready (see [What this project does not cover](#what-this-project-does-not-cover)).

## Learning objectives

After this episode you should be able to:

- Define a **nack**, **requeue** and a **poison message**.
- Explain why a message rejected with `requeue = true` can come back forever, and why it blocks the messages behind it.
- Choose between `requeue = true` and `requeue = false` for a failing message.
- Recognize a poison-message loop from the log and from the queue.
- Say what `requeue = false` does to the message when nothing else is configured.

## From the video to the code

| Video scene | Concept | Where to see it |
|---|---|---|
| Cold open | One order with no email address blocks the queue | [1. The problem](#1-the-problem-one-message-that-can-never-succeed) |
| Recap | A delivery stays unacknowledged until the consumer answers | [Episode 08](../08-rabbitmq-manual-ack/) |
| Nack and requeue | `basicNack` and its third argument | [`Consumer.java`](src/main/java/com/fromdevtodev/rabbitmq/nack/Consumer.java), [`Rules.java`](src/main/java/com/fromdevtodev/rabbitmq/nack/Rules.java), [Experiment 1](#experiment-1-the-requeue-loop) |
| The loop | Redelivered flag, poison message | [3. What requeue really does](#3-what-requeue-really-does) |
| The other choice | `requeue = false` | [Experiment 2](#experiment-2-requeue-false) |
| Choosing | Failures that pass and failures that never will | [`EmailWorker.java`](src/main/java/com/fromdevtodev/rabbitmq/nack/EmailWorker.java), [Experiment 3](#experiment-3-choose-by-kind-of-failure) |
| Misconception | "Requeue means try again later" | [5. The misconception](#5-the-misconception-requeue-means-try-again-later) |

## 1. The problem: one message that can never succeed

The exchange `orders` is a direct exchange. The queue `notifications.email` is bound to it with the key `order.created`, and one email worker reads that queue. Three orders arrive:

| Message | Order | Email address |
|---|---|---|
| `evt_0001` | `ord_1041` | `customer0001@example.com` |
| `evt_0002` | `ord_1042` | **none** |
| `evt_0003` | `ord_1043` | `customer0003@example.com` |

The worker sends the first email and acknowledges it. The second order has no email address, so the worker cannot process it, and whatever it answers next decides the fate of the whole queue.

## 2. Nack and requeue

After a delivery the worker answers RabbitMQ. We already know the positive answer, an **ack**: the work is done and the message can be deleted. The negative answer is a **nack** (a negative acknowledgement): it tells RabbitMQ that the delivery was **not processed**. The AMQP 0-9-1 specification has an older method for the same purpose, `basic.reject`, which handles one message at a time; `basic.nack` is a RabbitMQ extension that adds the option of rejecting several deliveries in one call.

A nack carries one more choice, **requeue**:

- `requeue = true`: RabbitMQ puts the message back into the queue, to be delivered again. RabbitMQ places a requeued message in its original position in the queue, if possible.
- `requeue = false`: the message leaves the queue. RabbitMQ discards it, or dead-letters it if the queue is configured to do so (dead-letter exchanges are the subject of episode 13).

In the Java client, a nack is one call:

```java
channel.basicNack(deliveryTag, false, true);   // delivery, "up to this one"?, requeue
```

The second argument says whether to nack every unacknowledged delivery up to this one; this project always passes `false`.

## 3. What requeue really does

Take the queue of the story, with one consumer that takes **one message at a time** (`channel.basicQos(1)` in `Consumer.java`). `evt_0002` is at the head of the queue. The worker nacks it with `requeue = true`, and RabbitMQ puts it back where it was, at the head. The next delivery is therefore `evt_0002` again. It fails the same way, because the message is the same, and the cycle repeats. `evt_0003` stays ready, one place behind, for as long as the loop lasts.

A message that fails every time it is processed is a **poison message**. Nothing in this exchange ever changes its outcome: the failure is in the message, not in the environment.

Two details help you recognize the loop:

- A message delivered again carries the **redelivered flag**. RabbitMQ documents it as a hint that the consumer *may* have seen the message before; it says "seen before", not how many times. This project also counts attempts per event, in memory, to make the loop visible; a restart of the worker starts the counts again.
- Requeueing has no delay, no backoff and no counter of its own. The message is delivered again as soon as the consumer is ready for it.

This demo uses a **classic** queue, declared explicitly in `Topology.java`. On a classic queue nothing stops the loop: it ends only when the message is removed or the failure goes away. Quorum queues, which have their own poison-message protection, are a topic for a later episode.

## 4. The other choice: requeue false

With `requeue = false` the message leaves the queue, and the queue moves on. In this project **no dead-letter exchange is configured**, so the message is simply discarded: no error, no copy. That ends the loop, and it also loses the message.

## 5. The misconception: "requeue means try again later"

It is natural to read *requeue* as "put it at the back of the line and try again later, after the others". That is not what happens. The message goes back to its **original position**, so if it was first, it is first again, and it is delivered again straight away. There is no waiting time, and the messages behind it wait for as long as it keeps failing.

## Choosing: will this failure pass by itself?

| Failure | Passes by itself? | Answer |
|---|---|---|
| The mail server is briefly unavailable, a database restarts, a dependency is down for a minute | Yes | `requeue = true`, with care: a failure that never passes is a loop too |
| The order has no email address, the payload is malformed, the event type is unknown | No | `requeue = false`: the same message will fail the same way |
| You are not sure | — | Do not requeue blindly forever: park the message and raise an alert (episodes 13 and 15) |

## When *not* to use each choice

- **Do not requeue bad data.** It only builds a loop.
- **Do not use `requeue = false` on a queue with no dead-letter setup** for messages you need: they are gone.
- **Do not expect requeue to wait or to count.** If you need a delay or a retry limit, you have to build them (episode 15 covers bounded retries).

## Hands-on experiments

### Prerequisites

- Java 21, Maven, Docker with Compose.
- Start RabbitMQ and build the project:

```bash
docker compose up -d rabbitmq
mvn -q -DskipTests compile
```

The management UI is at <http://localhost:15672> (`guest` / `guest`). Every command below is `mvn -q exec:java -Dexec.args="..."`; the examples abbreviate it to `App ...`.

| Command | What it does |
|---|---|
| `App setup` | Declares the exchange `orders`, the classic queue `notifications.email` and the binding `order.created` |
| `App publish [--first 1] [--count 3] [--poison 2]` | Publishes `count` events starting at number `first`; the one numbered `poison` has no email address (`--poison 0`: none) |
| `App consume [--on-failure requeue\|discard\|by-kind] [--work-ms 250] [--flaky 0] [--limit 0]` | The email worker. It takes one message at a time and answers each: ack or nack. `--on-failure` chooses what a nack asks for (see below). `--flaky N` makes the first N attempts of every event fail with a transient error. `--limit N` stops after N answered deliveries (0: until Ctrl+C) |
| `App reset` | Deletes the exchange and the queue |

With `--on-failure`: `requeue` always nacks with `requeue = true`; `discard` always nacks with `requeue = false`; `by-kind` requeues only transient failures and never an invalid order.

To look at the broker yourself, in another terminal:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

`messages_ready` counts the messages waiting to be delivered, and `messages_unacknowledged` those that were delivered and are not answered yet.

### Experiment 1: the requeue loop

```bash
App reset && App setup
App publish --first 1 --count 3 --poison 2
App consume --on-failure requeue          # stop it with Ctrl+C after a few seconds
```

Expected: `evt_0001` is processed on the first attempt and acknowledged. Then `evt_0002` is printed again and again, with the attempt number climbing and `redelivered=true` after the first line, each one followed by `nack, requeue=true`. `evt_0003` never appears. While the worker runs, `rabbitmqctl list_queues` keeps showing the queue with messages in it: the queue does not drain.

### Experiment 2: requeue false

```bash
App reset && App setup
App publish --first 1 --count 3 --poison 2
App consume --on-failure discard --limit 3
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

Expected: `evt_0001` is acknowledged, `evt_0002` is nacked with `requeue=false` on its first attempt and is **not** delivered again, and `evt_0003` is acknowledged. The queue ends with 0 ready and 0 unacknowledged messages: the loop is gone, and so is `evt_0002`.

### Experiment 3: choose by kind of failure

```bash
App reset && App setup
App publish --first 1 --count 3 --poison 2
App consume --on-failure by-kind --flaky 2 --limit 7
```

Expected: `evt_0001` fails twice with a transient error (`nack, requeue=true`) and is acknowledged on its third attempt; `evt_0002` is nacked once with `requeue=false`, because an order with no email address will never pass; `evt_0003` behaves like `evt_0001`. That is seven answered deliveries, and the queue is empty at the end. (This experiment is not part of the video recordings.)

> The output above describes what the code is written to do. If your broker version prints something different, trust your broker and the [official documentation](#sources).

## Project structure

```text
09-rabbitmq-nack-and-requeue/
├── docker-compose.yml
├── pom.xml
├── recordings/               # scripts that record the video demos (maintainers)
└── src/
    ├── main/java/com/fromdevtodev/rabbitmq/nack/
    │   ├── App.java          # command line
    │   ├── Rules.java        # names, and our model of what the worker answers: ack, or nack with requeue true/false
    │   ├── EmailWorker.java  # the business work: succeeds, or fails with an invalid-order or a transient error
    │   ├── Consumer.java     # the worker: one message at a time, ack or nack
    │   ├── Publisher.java    # publishes order.created events; one can have no email address
    │   ├── Topology.java     # exchange, queue and binding declarations, and the connection settings
    │   ├── Bindings.java     # setup, reset
    │   ├── OrderEvent.java   # the message
    │   └── ...               # Args, Attempts, EventJson, Log, Report
    └── test/java/...         # tests for the parts that need no broker
```

### Tests

```bash
mvn test
```

The tests cover the parts that need no running broker: what the worker answers for each policy (`Rules.decide`), the email worker's failures, the attempt counter, the log line, the message and argument parsing. What the broker does with a nack (put the message back, discard it) is shown by the experiments above, not by the tests.

## What this project does not cover

- **Dead-letter exchanges:** episode 13. They are where a message rejected with `requeue = false` can go instead of being discarded.
- **Delayed and bounded retries:** episodes 14 and 15. Here a requeue has no delay and no limit of its own.
- **Prefetch:** episode 10. The worker uses `basicQos(1)` only so that the effect of the loop is easy to see.
- **Quorum queues,** which add their own poison-message handling, and **negative acknowledgements in bulk** (the `multiple` flag).
- **Idempotent consumers:** a message that is delivered again can be processed twice; making that harmless is a later episode.
- **Metrics and alerts** for redelivery rates.

## Recap

- **Requeue puts a rejected message straight back, so a message that always fails is delivered again and again.**
- A nack tells RabbitMQ the delivery was not processed; its `requeue` argument decides whether the message goes back to the queue or leaves it.
- A poison message fails every time, so requeue turns it into a loop that blocks the queue behind it.
- `requeue = false` ends the loop, and without a dead-letter exchange it also discards the message.
- Choose by asking whether the failure will pass by itself.

## Sources

The claim-by-claim verification notes live in the video repository (`docs/sources/rabbitmq-nack-and-requeue-sources.md`). Official references:

- [RabbitMQ: Negative Acknowledgements](https://www.rabbitmq.com/docs/nack)
- [RabbitMQ: Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [RabbitMQ: Reliability Guide](https://www.rabbitmq.com/docs/reliability)
- [RabbitMQ: Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
