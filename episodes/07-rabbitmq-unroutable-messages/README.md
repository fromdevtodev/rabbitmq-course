# 07 — Unroutable messages: when RabbitMQ drops yours without an error

**Level:** Engineer  
**Episode:** Unroutable messages: when RabbitMQ drops yours without an error  
**Language:** English  
**Code:** Java 21 · Maven · RabbitMQ Java client · Docker Compose  
**Requires:** [Episode 04 — Direct exchange](../04-rabbitmq-direct-exchange/)

## YouTube

[Watch this episode]({{youtubeUrl}})

This folder is the companion project for the seventh episode of the course. The README is the **written version of the lesson**: together with the video it should leave the concept complete. Read it to review the ideas, then run the experiments yourself.

> **The one sentence of this episode**
>
> **A message that matches no binding is dropped silently unless you ask RabbitMQ to give it back.**

"Give it back" has two forms, and they are the two standard defenses: the **mandatory flag** returns the message to the publisher, and an **alternate exchange** hands it to another exchange inside the broker.

> **Learning-project disclaimer:** the queues are just queues: `notifications.email` sends no email and `audit.log` stores no log. The `consume` command stands in for a consumer so that you can see what a queue holds. The three terminal demos in the video are **real recordings** of this project. The code is illustrative, not production-ready (see [What this project does not cover](#what-this-project-does-not-cover)).

## Learning objectives

After this episode you should be able to:

- Define an **unroutable message**, the **mandatory flag** and an **alternate exchange**.
- Explain why a publish call can succeed and the message still reach no queue.
- Publish with the mandatory flag and handle the return in a return listener.
- Attach an alternate exchange to an exchange with a policy, with a queue behind it.
- Choose between the two defenses, and say what neither of them guarantees.

## From the video to the code

| Video scene | Concept | Where to see it |
|---|---|---|
| Cold open | A cancellation email that never arrives | [1. The problem](#1-the-problem-one-wrong-letter-zero-queues) |
| Recap and failure | Bindings, exact matching, the typo with one L | [`Rules.java`](src/main/java/com/fromdevtodev/rabbitmq/unroutable/Rules.java), [Experiment 1](#experiment-1-the-silent-drop) |
| Why nobody tells you | Routing happens after the publish call returns | [2. Why the publisher hears nothing](#2-why-the-publisher-hears-nothing) |
| Mandatory flag | Return listener | [`Publisher.java`](src/main/java/com/fromdevtodev/rabbitmq/unroutable/Publisher.java), [Experiment 2](#experiment-2-the-mandatory-flag) |
| Alternate exchange | A fanout exchange and a parking queue, attached with a policy | [`Topology.java`](src/main/java/com/fromdevtodev/rabbitmq/unroutable/Topology.java), [Experiment 3](#experiment-3-the-alternate-exchange) |
| Misconception | "No exception means it reached a queue" | [4. The misconception](#4-the-misconception-no-exception-means-it-arrived) |

## 1. The problem: one wrong letter, zero queues

The exchange `orders` is a direct exchange with five bindings:

| Queue | Binding keys |
|---|---|
| `notifications.email` | `order.created`, `order.cancelled` |
| `loyalty.points` | `order.created` |
| `audit.log` | `order.created`, `order.cancelled` |

The order service publishes `order.canceled`, with **one L**. The bindings use two. A direct exchange compares keys by exact equality, so no binding matches. The message is an **unroutable message**: it matches no binding, so there is no queue to copy it to.

When a published message cannot be routed to any queue and the publisher did not set the mandatory flag (the default), RabbitMQ discards it, or republishes it to an alternate exchange if one is configured. The publisher gets no error.

## 2. Why the publisher hears nothing

The Java client's `basicPublish` writes the message to the connection and returns. Routing happens inside the broker, afterwards. Nothing in the protocol obliges the broker to report a message that matched no queue, so by default it does not. An exchange keeps no messages either, so there is nothing to wait for and nothing to inspect later.

## 3. The two defenses

### The mandatory flag: the publisher is told

`basicPublish(exchange, routingKey, mandatory, props, body)` takes a `mandatory` argument. When it is `true` and the message is unroutable, the broker returns the message to the publisher, with a reply code and a reply text. The client library delivers it, asynchronously, to a **return listener** registered on the channel:

```java
channel.addReturnListener((code, text, exchange, key, props, body) ->
    log("returned: " + text + ", key " + key));

channel.basicPublish("orders", "order.canceled", true, props, body);
```

The publish call itself still returns normally; the listener runs afterwards. A publisher that sets the flag but registers no listener has nobody to handle the return, so the two always go together. The message is not stored anywhere: the application decides what to do (log, count, retry with a corrected key, fail the request).

### The alternate exchange: the broker keeps it

An **alternate exchange** (AE) is an exchange that another exchange hands a message to when it cannot route it. Put a queue behind the AE and the strays pile up there. The producer is not involved. This project uses a **fanout** exchange as the AE, which copies every message it receives to all its queues, whatever the key:

- exchange `orders.unrouted` (fanout)
- queue `unrouted.parking`, bound to it

You attach the AE to `orders` with a **policy** (RabbitMQ's recommended way). The alternate exchange and its queue must exist; if the AE does not exist, RabbitMQ only logs a warning.

```bash
docker compose exec rabbitmq rabbitmqctl set_policy AE "^orders$" \
  '{"alternate-exchange":"orders.unrouted"}' \
  --apply-to exchanges
```

Setting `alternate-exchange` as an argument when the exchange is declared also works, but RabbitMQ discourages it, and if both a policy and an argument define an AE, the argument wins.

A message routed through an AE still counts as routed for the purpose of the mandatory flag. So with both in place, the alternate exchange catches the message and **no return is sent**.

## 4. The misconception: "no exception means it arrived"

A publish call that returns without an exception says only that the client wrote the message to the connection. It does not say that a queue holds a copy. Even a publisher confirm (episode 12) does not tell you that a queue received the message. If you need to know a message was routed, ask the broker: the mandatory flag, an alternate exchange, or both.

## When *not* to use them

- **Dropping is fine** (optional, low-value events): neither is needed.
- **An unwatched parking queue** only hides the problem. Alert when it is not empty.
- **Neither is a delivery guarantee.** A message that reached a queue and then fails is a different topic (dead-letter exchanges, episode 13).

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
| `App setup` | Declares the exchange `orders`, the queues `notifications.email`, `loyalty.points`, `audit.log` and the five bindings |
| `App setup-parking` | Declares the exchange `orders.unrouted` (fanout) and the queue `unrouted.parking`. It does **not** attach them to `orders` |
| `App publish --key K [--mandatory true] [--count 1] [--first 1]` | Publishes to `orders` with routing key `K`; with `--mandatory true` it also prints any return |
| `App predict --key K` | Our own model of the rule: which queues should get a copy (the broker has the last word) |
| `App consume --queue Q [--limit 0]` | Prints the messages of one queue (`--limit 0`: until Ctrl+C) |
| `App status [--queues a,b] [--samples 1]` | Prints the ready messages of each queue |
| `App reset` | Deletes both exchanges and the queues. It does **not** remove a policy (see below) |

To look at the broker yourself:

```bash
docker compose exec rabbitmq rabbitmqctl list_bindings source_name routing_key destination_name
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready
docker compose exec rabbitmq rabbitmqctl list_policies
```

### Experiment 1: the silent drop

```bash
App reset && App setup
App predict --key order.canceled          # our model: no binding matches
App publish --key order.created  --first 1
App publish --key order.canceled --first 2
App status
```

Expected: the first message gives each of the three queues one ready message. The second is reported as published, and no counter moves. Compare with `App predict --key order.cancelled` (two Ls).

### Experiment 2: the mandatory flag

```bash
App publish --key order.canceled --mandatory true --first 3
```

Expected: a `RETURNED evt_0003` line with the reply code and reply text the broker sent, the exchange and the routing key, followed by "1 message(s) were returned". `App status` still shows the queues unchanged: the message came back to the publisher, it was not delivered. (The exact reply text is whatever your broker returns.)

### Experiment 3: the alternate exchange

```bash
App setup-parking
docker compose exec rabbitmq rabbitmqctl set_policy AE "^orders$" \
  '{"alternate-exchange":"orders.unrouted"}' --apply-to exchanges
App publish --key order.canceled --first 4
App status --queues unrouted.parking
App consume --queue unrouted.parking --limit 1
App publish --key order.canceled --mandatory true --first 5
```

Expected: `unrouted.parking` holds the message, and `consume` prints it with the routing key `order.canceled`. The mandatory publish is parked as well and prints "no message was returned", because a message routed through an alternate exchange counts as routed.

To remove the policy afterwards:

```bash
docker compose exec rabbitmq rabbitmqctl clear_policy AE
```

> The output above describes what the code is written to do. If your broker version prints something different, trust your broker and the [official documentation](#sources).

## Project structure

```text
07-rabbitmq-unroutable-messages/
├── docker-compose.yml
├── pom.xml
├── recordings/               # scripts that record the video demos (maintainers)
└── src/
    ├── main/java/com/fromdevtodev/rabbitmq/unroutable/
    │   ├── App.java          # command line
    │   ├── Rules.java        # names, the five bindings, and our model of the matching rule (no broker)
    │   ├── Topology.java     # exchange, queue and binding declarations, and the connection settings
    │   ├── Bindings.java     # setup, setup-parking, reset
    │   ├── Publisher.java    # publish to orders, with or without the mandatory flag; the return listener
    │   ├── Consumer.java     # prints the messages of one queue
    │   ├── QueueStatus.java  # passive declare: ready messages per queue
    │   ├── OrderEvent.java   # the message
    │   └── ...               # Args, EventJson, Log, StatusLine
    └── test/java/...         # tests for the parts that need no broker
```

### Tests

```bash
mvn test
```

The tests cover the parts that need no running broker: the rule model in `Rules` (which keys are routable, exact matching, the typo), the message, the status line and argument parsing. The broker's behavior (silent drop, return, alternate exchange) is shown by the experiments above, not by the tests.

## What this project does not cover

- **Publisher confirms:** episode 12. The publisher here waits a fixed second for a possible return instead of using confirms.
- **Dead-letter exchanges:** episode 13. They handle messages that *were* routed to a queue and then rejected or expired.
- **Alternate-exchange chains, and AEs for other protocols.**
- **Metrics and alerts** for dropped or returned messages, and what to do with the parked messages.
- **Durability and persistence** of messages, queues and exchanges.

## Recap

- **A message that matches no binding is dropped silently unless you ask RabbitMQ to give it back.**
- By default the publisher is not told; the publish call only says the message was written to the connection.
- The mandatory flag returns the message to the publisher, which needs a return listener.
- An alternate exchange, attached with a policy, lets the broker keep the message in a queue you can watch.
- With an alternate exchange that routes the message, nothing is returned.

## Sources

The claim-by-claim verification notes live in the video repository (`docs/sources/rabbitmq-unroutable-messages-sources.md`). Official references:

- [RabbitMQ: Publishers](https://www.rabbitmq.com/docs/publishers)
- [RabbitMQ: Alternate Exchanges](https://www.rabbitmq.com/docs/ae)
- [RabbitMQ: Introducing Publisher Confirms (blog)](https://www.rabbitmq.com/blog/2011/02/10/introducing-publisher-confirms)
- [rabbitmqctl reference](https://www.rabbitmq.com/docs/man/rabbitmqctl.8)
