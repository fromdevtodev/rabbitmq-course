# 06 — Fanout exchange: broadcast to every queue

**Level:** Engineer  
**Episode:** Fanout exchange: broadcast to every queue  
**Language:** English  
**Code:** Java 21 · Maven · RabbitMQ Java client · Docker Compose  
**Requires:** [03 — Exchanges and bindings](../03-rabbitmq-exchanges-and-bindings/README.md)  
**Reading time:** about 12 minutes, plus the experiments

## YouTube

https://www.youtube.com/watch?v=xAGYA_aXarI

This README is the **written version of the lesson**. It tells the same story as the video, with more room for the reasoning and for the code. You can read it before the video, after it, or instead of it. Everything it describes can be run on your machine with `./run.sh`.

> **The one sentence of this episode**
>
> **A fanout exchange ignores the routing key and copies every message to every queue bound to it.**

> **Learning-project disclaimer:** nothing here caches anything or stores a real price. The messages only wait in the queues, or are printed by the consumers, so that you can count them. The terminal demos in the video run this project against a real RabbitMQ. The code is illustrative, not production-ready (see [What this project does not cover](#what-this-project-does-not-cover)).

## Learning objectives

After this episode you should be able to:

- Define **fanout exchange**, **broadcast** and **exclusive queue**.
- Say which queues receive a copy of a message published to a fanout exchange, whatever the routing key and the binding keys are.
- Explain why twelve consumers on one queue are **not** a broadcast, and what twelve instances need instead.
- Declare a queue that belongs to one instance alone, with a name chosen by the broker, and bind it to a fanout exchange.
- Say what happens to a message published to a fanout exchange with no queue bound.
- Read the code of the demos and say which call declares the fanout exchange, which call creates a queue for one instance and which call carries the routing key.

## Contents

1. [The problem: twelve caches, one updated](#the-problem-twelve-caches-one-updated)
2. [The vocabulary](#the-vocabulary)
3. [One queue is not a broadcast](#one-queue-is-not-a-broadcast)
4. [Experiment 1: watch the keys fail to matter](#experiment-1-watch-the-keys-fail-to-matter)
5. [Experiment 2: one queue, or one each](#experiment-2-one-queue-or-one-each)
6. [The rule](#the-rule)
7. [Read the code](#read-the-code)
8. [Try it yourself](#try-it-yourself), [where it fits](#where-it-fits), [things that surprise people](#things-that-surprise-people), [summary](#summary) and [what this project does not cover](#what-this-project-does-not-cover)

| Video scene | Where it is in this README |
|---|---|
| Cold open, "A price changes, one cache notices" | [The problem](#the-problem-twelve-caches-one-updated) |
| "Broadcast to every queue", "One queue, twelve instances", "One publish, one copy for each queue" | [The vocabulary](#the-vocabulary), [One queue is not a broadcast](#one-queue-is-not-a-broadcast) |
| Challenge: which queues get this message, board and demo "A fanout exchange ignores the key" | [Experiment 1](#experiment-1-watch-the-keys-fail-to-matter) |
| "Who reads the routing key?", "More consumers, or more queues?" | [The rule](#the-rule), [One queue is not a broadcast](#one-queue-is-not-a-broadcast) |
| "Fanout exchange: four calls", "The life of an exclusive queue" | [Read the code](#read-the-code) |
| Board and demo "One queue, or one queue each" | [Experiment 2](#experiment-2-one-queue-or-one-each) |
| Challenge: twelve instances, "Three steps to broadcast", "When it fits" | [Try it yourself](#try-it-yourself), [Where it fits](#where-it-fits) |
| Recap | [Summary](#summary) |

## The problem: twelve caches, one updated

A pricing service changes the price of a product. Twelve instances of the catalog service each keep that price in a cache, and **every one of them** must drop its copy when the price changes. The first idea is to put the price change in a queue and let the twelve instances consume from it.

It does not work: RabbitMQ delivers each message of a queue to **one** consumer. One instance drops its cached price; the other eleven never see the message and keep serving the old one. The messages are not lost. They went to the wrong place for this job: the job needs **a copy for every instance**.

## The vocabulary

You know these from episodes 3 and 4: a producer publishes to an **exchange**; a **binding** (an exchange, a key and a queue) tells the exchange which queues may receive a copy; the **routing key** is the label of the message.

Three new terms:

- **Fanout exchange**: an exchange type that copies every message to every queue bound to it, and ignores the routing key.
- **Broadcast**: one publish, and every receiver gets its own copy. With a fanout exchange the receivers are **queues**.
- **Exclusive queue**: a queue that only one connection can use, and that the broker deletes when that connection closes. An instance that declares one with an empty name lets the broker choose the name.

## One queue is not a broadcast

Compare the two designs the project runs in [experiment 2](#experiment-2-one-queue-or-one-each):

| | One queue, many consumers | One queue for each receiver |
|---|---|---|
| Copies made by the exchange | one (one queue is bound) | one for each bound queue |
| Who gets a message | **one** of the consumers: they compete | **every** receiver, from its own queue |
| Good for | sharing work | broadcasting |

**The exchange makes one copy per bound queue, never per consumer.** More consumers on a queue do not make a broadcast. More queues do.

## Experiment 1: watch the keys fail to matter

Three queues are bound to the fanout exchange `price-updates` with three different binding keys. Three price changes are published, with three routing keys.

```bash
./run.sh up            # RabbitMQ in Docker, project compiled
./run.sh experiment1
```

| Queue | Bound with |
|---|---|
| `cache.instance-1` | an empty key |
| `cache.instance-2` | `books` |
| `cache.instance-3` | `electronics` |

The experiment prints the bindings and the ready messages of the three queues, then publishes `evt_0001` with the key `toys`, `evt_0002` with `books` and `evt_0003` with `electronics`. After the first publish every queue holds one message, **although no binding key says `toys`**. After the second every queue holds two, and after the third, three: instance one and instance three get a copy of the `books` message even though their bindings say something else.

The service published three times and never named a queue. The exchange made one copy for each queue bound to it, whatever the keys said. A fourth queue bound with any key would get a copy too, with no change in the service.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/Topology.java#bindings -->
```java
static final List<Binding> BROADCAST = List.of(
        new Binding(INSTANCE_1, ""),
        new Binding(INSTANCE_2, "books"),
        new Binding(INSTANCE_3, "electronics"));
```
<!-- /snippet -->

## Experiment 2: one queue, or one each

Two designs, side by side.

```bash
./run.sh experiment2
```

**Design one: one queue, two consumers.** The queue `cache.shared` is bound to the exchange, and two consumers, `cache-a` and `cache-b`, listen to it. Four price changes are published. Four messages are received in total, and **each message reaches exactly one consumer, never both**. How the four split between the two is not the point, and it can differ from run to run.

**Design two: one queue each.** The consumers stop, the broker is cleared (so `cache.shared` is not bound any more), and two instances start. Each one declares **its own exclusive queue**, with a name chosen by the broker, and binds it to the exchange. Four price changes are published again. Eight receptions happen, for only four messages: **every message arrives twice, once at each instance**, so each instance receives all four, because each instance has its own queue and the exchange made one copy for each.

When the instances stop, the broker deletes their queues with their connections. A queue like this is not kept while no instance runs, so a price change published in that time is never seen by an instance that starts later.

The experiment checks each of these statements and stops with an error message if one is false.

## The rule

> A fanout exchange ignores the routing key and copies every message to every queue bound to it.

- **The routing key is ignored.** You can publish with any key, or with an empty one.
- **The binding keys are ignored too.** A binding is just the link between the exchange and a queue. An empty key is not special.
- **One copy for each bound queue.** Not for each consumer.
- **No queue bound, nobody to copy to.** The message is discarded and the publish does not fail ([experiment 3](#try-it-yourself)). What to do about it is the next episode.

The rule as plain Java, so that you can read it and test it (RabbitMQ does the real routing):

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/FanoutRule.java#rule -->
```java
/**
 * The queues that receive a copy of a message: every queue bound to the exchange. The routing key of the message is
 * accepted and ignored, and so are the binding keys. (Each queue of this project is bound once; what the broker does with
 * one queue bound twice is not covered here.)
 */
static List<String> queuesFor(String routingKey, List<Binding> bindings) {
    return bindings.stream().map(Binding::queue).toList();
}
```
<!-- /snippet -->

## Read the code

| File | Role |
|---|---|
| `App.java` | Entry point: one command per step of the experiments |
| `Topology.java` | The exchange, the queue names, the table of bindings and the calls that declare and bind |
| `FanoutRule.java` | The rule of the exchange written as code, so that you can read and test it |
| `Publisher.java` | The producer: publishes to `price-updates` with a routing key it does not need |
| `Consumers.java` | The receivers of experiment 2: a consumer of a shared queue, and an instance with its own exclusive queue |
| `Setup.java` | Declares the starting point of each experiment |
| `Binding.java`, `PriceUpdate.java`, `EventJson.java`, `Args.java`, `Log.java` | Small supporting pieces; you can skip them |

Follow the execution:

**1. The entry point** maps each command to one class.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/App.java#commands -->
```java
switch (args[0]) {
    case "setup" -> Setup.broadcast();
    case "setup-shared" -> Setup.shared();
    case "setup-empty" -> Setup.empty();
    case "publish" -> Publisher.publish(
            options.text("key", "toys"),
            options.number("count", 1),
            options.number("first", 1));
    case "consume" -> Consumers.onSharedQueue(options.required("name"), options.text("queue", Topology.SHARED));
    case "instance" -> Consumers.instance(options.required("name"));
    case "explain" -> explain(options.text("key", "toys"));
    case "reset" -> Topology.reset();
```
<!-- /snippet -->

**2. The exchange.** The type is the only thing that makes it a fanout exchange.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/Topology.java#exchange -->
```java
static void declareExchange(Channel channel) throws IOException {
    channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT, true);
}
```
<!-- /snippet -->

**3. A queue and its binding.** Two calls, as in every episode: declare the queue, bind it. The key (`""`, `"books"`, `"electronics"`) changes nothing.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/Topology.java#bind -->
```java
static void declareAndBind(Channel channel, String queue, String key) throws IOException {
    channel.queueDeclare(queue, true, false, false, null);
    channel.queueBind(queue, EXCHANGE, key);
}
```
<!-- /snippet -->

**4. A queue for one instance.** An empty name asks the broker to choose one, and the client uses the name that comes back. The queue is exclusive (the third argument) and the broker deletes it when the connection closes.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/Topology.java#exclusive -->
```java
static String declareExclusiveAndBind(Channel channel) throws IOException {
    String queue = channel.queueDeclare("", false, true, true, null).getQueue();
    channel.queueBind(queue, EXCHANGE, "");
    return queue;
}
```
<!-- /snippet -->

**5. The producer.** It names an exchange and a key. It does not know how many queues are bound, how many consumers listen, or what the bindings say.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/Publisher.java#publish -->
```java
/** Publishes price changes {@code first .. first + count - 1} to the exchange price-updates, all with the given key. */
static void publish(String routingKey, long count, long first) throws IOException, TimeoutException {
    try (Connection connection = Topology.connect("pricing-service");
         Channel channel = connection.createChannel()) {
        Topology.declareExchange(channel);
        for (long seq = first; seq < first + count; seq++) {
            PriceUpdate update = PriceUpdate.of(seq, routingKey);
            channel.basicPublish(Topology.EXCHANGE, routingKey, properties(update), EventJson.toBytes(update));
            Log.info("pricing-service", "published " + update.eventId() + " to '" + Topology.EXCHANGE
                    + "' with routing key " + routingKey);
        }
    }
}
```
<!-- /snippet -->

**6. An instance.** It declares its exchange (a harmless repeat), its own queue, and a consumer for it. The consumer prints one line for each message it receives.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/fanout/Consumers.java#instance -->
```java
/** One instance of the catalog service: its own exclusive queue, bound to the exchange, and a consumer for it. */
static void instance(String name) throws IOException, TimeoutException, InterruptedException {
    Connection connection = Topology.connect(name);
    closeOnExit(connection);
    Channel channel = connection.createChannel();
    Topology.declareExchange(channel);
    String queue = Topology.declareExclusiveAndBind(channel);
    Log.info(name, "ready on queue " + queue);
    listen(name, channel, queue);
    new CountDownLatch(1).await();
}
```
<!-- /snippet -->

> **Ignore for now:** the consumers of this project acknowledge messages automatically (the `true` in `basicConsume`). Manual acknowledgements, and what they protect you from, are a later episode.

## Try it yourself

1. **Prove the rule without a broker.** `./run.sh test` runs the unit tests, which state the rule for the three queues of experiment 1:

   <!-- snippet: src/test/java/com/fromdevtodev/rabbitmq/fanout/FanoutExchangeTest.java#test -->
```java
@Test
void everyBoundQueueGetsACopyWhateverTheKey() {
    List<String> all = List.of("cache.instance-1", "cache.instance-2", "cache.instance-3");
    assertEquals(all, FanoutRule.queuesFor("toys", Topology.BROADCAST));
    assertEquals(all, FanoutRule.queuesFor("books", Topology.BROADCAST));
    assertEquals(all, FanoutRule.queuesFor("", Topology.BROADCAST));
}

@Test
void theBindingKeysMakeNoDifference() {
    List<Binding> sameKeys = List.of(
            new Binding("cache.instance-1", "x"),
            new Binding("cache.instance-2", "x"),
            new Binding("cache.instance-3", "x"));
    assertEquals(FanoutRule.queuesFor("toys", Topology.BROADCAST), FanoutRule.queuesFor("toys", sameKeys));
}

@Test
void withNoQueueBoundThereIsNobodyToCopyTo() {
    assertEquals(List.of(), FanoutRule.queuesFor("toys", List.of()));
}
```
<!-- /snippet -->

2. **Experiment 3: nobody to copy to.** `./run.sh experiment3` declares the exchange with no queue bound and publishes one price change. The publish succeeds, and no queue holds anything: the message was discarded. *Why does the publisher not get an error?* Because by default it does not ask for one: the next episode is about that.
3. **A fourth queue.** Make a queue `cache.instance-4` and bind it with the key `anything`. Which messages does it receive? *Answer:* every message published **after** it is bound, whatever the key. Add it to a copy of `Setup`, run it, publish `--key toys` and look at `./run.sh counters`.
4. **Twelve instances.** Which setup lets twelve instances each drop a cached price, without the producer knowing how many instances exist? *Answer:* a fanout exchange, and each instance binds **its own** queue. Twelve consumers sharing one queue share one copy; publishing twelve times makes the producer know every instance.
5. **Predict, then run.** Start `./run.sh app setup`, then `./run.sh app publish --key toys`, and say how many messages each queue holds before you run `./run.sh counters`.

## Where it fits

Use a fanout exchange when **every receiver must see every message**: twelve caches that must all drop the same price, several services that react to the same event, a notification that every screen must show. It fits when receivers come and go, because each has its own queue, and when the producer must not know how many receivers exist.

It is the wrong tool when only some messages should reach some queues: a direct exchange routes by exact key, and a topic exchange by pattern when the keys have a structure. It is also the wrong tool when **one worker should handle each message**: one queue with competing consumers does that. And a queue that belongs to one instance **dies with it**: a receiver that must not miss messages while it is down needs a durable queue of its own, which a later episode covers.

## Things that surprise people

- **Twelve consumers on one queue are not a broadcast.** They share the queue's messages: each message goes to one of them.
- **The key is ignored on both sides.** A message published with a key nobody mentions still reaches every queue, and an empty binding key is just another key.
- **A fanout exchange does not care how many queues exist.** Adding a queue changes who receives copies, not the producer.
- **An exclusive queue is for one instance.** It is deleted when the instance's connection closes: nothing waits for an instance that is down.
- **No queue bound, no error.** The message is discarded and the publish succeeds.
- **Port conflicts.** Another episode's RabbitMQ uses the same ports. Run `./run.sh down` in that episode's folder first.

## Summary

- A **fanout exchange** ignores the routing key and copies every message to every queue bound to it: a **broadcast**.
- The exchange makes one copy for each **queue**, never for each consumer: twelve instances need twelve queues.
- An **exclusive queue**, named by the broker, gives each instance a queue of its own, and goes away with its connection.

**Next episode:** *Unroutable messages*: what happens when no queue is bound, and how to make RabbitMQ tell you.

## What this project does not cover

- How headers exchanges route, and the exchange types provided by plugins.
- What happens to a message that reaches no queue beyond "it is discarded by default" (the next episode): the `mandatory` flag, returns and alternate exchanges.
- Consuming reliably: acknowledgements, redelivery and what happens when a consumer fails come later.
- Durability, durable per-receiver queues, publisher confirms, queue types and any RabbitMQ version-specific behaviour.
- A queue bound twice to the same fanout exchange.
- Production concerns: security, configuration and error handling are minimal on purpose.
