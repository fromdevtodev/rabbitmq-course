# 07 — Unroutable messages: when RabbitMQ drops yours without an error

**Level:** Engineer  
**Episode:** Unroutable messages: when RabbitMQ drops yours without an error  
**Language:** English  
**Code:** Java 21 · Maven · RabbitMQ Java client · Docker Compose  
**Requires:** [04 — Direct exchange](../04-rabbitmq-direct-exchange/README.md)  
**Reading time:** about 14 minutes, plus the experiments

## YouTube

https://www.youtube.com/watch?v=mYySTEDxHEE

This README is the **written version of the lesson**. It tells the same story as the video, with more room for the reasoning and for the code. You can read it before the video, after it, or instead of it. Everything it describes can be run on your machine with `./run.sh`.

> **The one sentence of this episode**
>
> **A message that matches no binding is dropped silently, unless you ask RabbitMQ to give it back.**

> **Learning-project disclaimer:** nothing here sends a real email or stores a real order. The messages only wait in the queues, so that you can count them. The terminal demos in the video run this project against a real RabbitMQ. The code is illustrative, not production-ready (see [What this project does not cover](#what-this-project-does-not-cover)).

## Learning objectives

After this episode you should be able to:

- Define **unroutable message**, **mandatory flag** and **alternate exchange**.
- Explain why a publish call that returns normally does not mean that a queue received the message.
- Say what a publisher sees, and what the queues hold, when it publishes a key that no binding has, with and without the mandatory flag.
- Attach an alternate exchange to an exchange with a policy, and say why the service does not need a redeploy.
- Name what neither fix covers.
- Read the code of the demos and say which call registers the return listener, which argument is the mandatory flag and which call creates the catcher.

## Contents

1. [The problem: a cancellation nobody heard](#the-problem-a-cancellation-nobody-heard)
2. [The vocabulary](#the-vocabulary)
3. [Experiment 1: the silent drop](#experiment-1-the-silent-drop)
4. [Experiment 2: ask for it back](#experiment-2-ask-for-it-back)
5. [Experiment 3: park it](#experiment-3-park-it)
6. [The rule](#the-rule)
7. [Read the code](#read-the-code)
8. [Try it yourself](#try-it-yourself), [where it fits](#where-it-fits), [things that surprise people](#things-that-surprise-people), [summary](#summary) and [what this project does not cover](#what-this-project-does-not-cover)

| Video scene | Where it is in this README |
|---|---|
| Cold open, "What the team believed" | [The problem](#the-problem-a-cancellation-nobody-heard) |
| "The ledger of the exchange orders", demo "A publish that looks fine" | [Experiment 1](#experiment-1-the-silent-drop) |
| "Dropped, with no error", "Why the producer hears nothing" | [The vocabulary](#the-vocabulary), [The rule](#the-rule) |
| "The mandatory flag", challenge "Same typo, with the flag", board and demo "The broker hands it back" | [Experiment 2](#experiment-2-ask-for-it-back) |
| "Two defences, two places", board and demo "A queue for what matches nothing" | [Experiment 3](#experiment-3-park-it) |
| Challenge "Compliance wants every unroutable event", "What the fixes cost" | [Try it yourself](#try-it-yourself), [Where it fits](#where-it-fits) |
| Recap | [Summary](#summary) |

## The problem: a cancellation nobody heard

A customer cancels order `ord_1002`. The order service publishes an event, and the publish call returns normally. No exception, no warning in any log. The cancellation email never arrives.

The belief behind the ticket is natural: *if the publish call did not fail, the message reached a queue.* It is false. A publish call says that the message was **sent** to the exchange. What the exchange does with it is another matter, and with a direct exchange it can do nothing at all.

The exchange in this project is `orders`, a direct exchange (episode 4) with three queues:

| Queue | Bound to `orders` with the key |
|---|---|
| `notifications.email` | `order.created`, `order.cancelled` |
| `loyalty.points` | `order.created` |
| `audit.log` | `order.created`, `order.cancelled` |

The service publishes `order.canceled`, with **one L**. Every binding spells it with two.

## The vocabulary

Three terms, each tied to a place in the project.

- **Unroutable message.** A message that matches no binding of the exchange it was published to: the exchange has no queue to send it to. In this project, `order.canceled` published to `orders`.
- **Mandatory flag.** A flag on `basicPublish` that tells the broker: if no queue takes this message, give it back to me. The publisher receives it in a **return listener**.
- **Alternate exchange.** A second exchange that an exchange hands every message it cannot route. Put a queue behind it and the unroutable messages are kept there. In this project: `orders.unrouted`, a fanout exchange, with the queue `unrouted.parking`.

The five bindings of the exchange, as the project declares them:

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/unroutable/Topology.java#bindings -->
```java
static final List<Binding> ORDERS = List.of(
        new Binding(EMAIL, "order.created"),
        new Binding(EMAIL, "order.cancelled"),
        new Binding(POINTS, "order.created"),
        new Binding(AUDIT, "order.created"),
        new Binding(AUDIT, "order.cancelled"));
```
<!-- /snippet -->

## Experiment 1: the silent drop

**Predict first.** The service publishes `evt_0001` with the key `order.created`, then `evt_0002` with `order.canceled`, both the normal way. What does the service log, and how many messages does each queue hold after each step? (To check the rule without a broker, `./run.sh app explain --key order.canceled` prints what the rule predicts. It is the rule written as code, not a measurement.)

```bash
./run.sh up           # RabbitMQ in Docker
./run.sh experiment1  # the whole experiment, step by step
```

<details>
<summary>The raw commands behind <code>./run.sh experiment1</code></summary>

```bash
./run.sh app setup                                          # the exchange orders, three queues, five bindings
./run.sh bindings                                           # the bindings of orders
./run.sh app publish --key order.created --first 1          # evt_0001
./run.sh app publish --key order.canceled --first 2         # evt_0002, one L
./run.sh counters
```

</details>

What the experiment checks (ready messages in `notifications.email`, `loyalty.points` and `audit.log`):

| After | email | points | audit |
|---|---|---|---|
| start | 0 | 0 | 0 |
| `evt_0001` (`order.created`) | 1 | 1 | 1 |
| `evt_0002` (`order.canceled`) | 1 | 1 | 1 |

**Why.** `order.created` has three bindings, so three queues get a copy. `order.canceled` has none. The exchange drops the message, the publish call returns normally and the service logs `the call returned normally`: there is nothing else to log. The cancellation email will never be sent, and nothing says so.

The publisher side of the experiment:

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/unroutable/Publisher.java#publish -->
```java
/** Publishes events {@code first .. first + count - 1} to the exchange orders, all with the given routing key. */
static void publish(String routingKey, long count, long first, boolean mandatory) throws IOException, TimeoutException, InterruptedException {
    try (Connection connection = Topology.connect("order-service");
         Channel channel = connection.createChannel()) {
        Topology.declareExchange(channel);
        if (mandatory) {
            channel.addReturnListener(Publisher::onReturn);
        }
        for (long seq = first; seq < first + count; seq++) {
            OrderEvent event = OrderEvent.of(seq, routingKey);
            channel.basicPublish(Topology.EXCHANGE, routingKey, mandatory, properties(event), EventJson.toBytes(event));
            Log.info("order-service", "published " + event.eventId() + " (" + event.orderId() + ") to '" + Topology.EXCHANGE
                    + "' with routing key " + routingKey + (mandatory ? ", mandatory" : "") + ": the call returned normally");
        }
        if (mandatory) {
            Thread.sleep(RETURN_GRACE_MILLIS);   // the return arrives after the call has returned: give it a moment
        }
    }
}
```
<!-- /snippet -->

## Experiment 2: ask for it back

**Predict first.** The same typo, now with the mandatory flag and a return listener. What does the service log? Do any queue receive the message? Then a routed message, also with the flag: does anything come back?

```bash
./run.sh experiment2
```

<details>
<summary>The raw commands behind <code>./run.sh experiment2</code></summary>

```bash
./run.sh app setup
./run.sh app publish --key order.canceled --first 1 --mandatory true     # evt_0001: comes back
./run.sh app publish --key order.created --first 2 --mandatory true      # evt_0002: routed, nothing comes back
./run.sh counters
```

</details>

| After | email | points | audit | Log |
|---|---|---|---|---|
| start | 0 | 0 | 0 | |
| `evt_0001` (`order.canceled`, mandatory) | 0 | 0 | 0 | `RETURNED evt_0001 ...` |
| `evt_0002` (`order.created`, mandatory) | 1 | 1 | 1 | no `RETURNED` line |

**Why.** With the flag, the broker hands the unroutable message back to the publisher. The queues still hold nothing: the flag does not keep the message anywhere, it only **tells the publisher**. The publish call itself still returns normally; the return arrives afterwards, as a separate callback. That is why the publisher waits a moment before closing its connection.

The listener that receives it:

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/unroutable/Publisher.java#return -->
```java
private static void onReturn(int replyCode, String replyText, String exchange, String routingKey,
                             AMQP.BasicProperties properties, byte[] body) throws IOException {
    OrderEvent event = EventJson.fromBytes(body);
    Log.info("order-service", "RETURNED " + event.eventId() + ": the broker found no queue for routing key " + routingKey
            + " on '" + exchange + "' (reply " + replyCode + ", " + replyText + ")");
}
```
<!-- /snippet -->

What to do with a returned message is the publisher's job: log it, publish it again with the right key, or raise an alert.

## Experiment 3: park it

**Predict first.** The broker is told to pass every message that matches no binding to `orders.unrouted`, a fanout exchange with the queue `unrouted.parking`. The service publishes `order.canceled` twice (the second time with the mandatory flag) and then `order.created`. Where does each message end up? Does the second one come back?

```bash
./run.sh experiment3
```

<details>
<summary>The raw commands behind <code>./run.sh experiment3</code></summary>

```bash
./run.sh app setup-parking                                  # orders, its queues, orders.unrouted and unrouted.parking
./run.sh policy                                             # attaches orders.unrouted to orders
./run.sh app publish --key order.canceled --first 1                       # evt_0001
./run.sh app publish --key order.canceled --first 2 --mandatory true      # evt_0002
./run.sh app publish --key order.created --first 3                        # evt_0003
./run.sh counters
```

The policy is this one command:

```bash
rabbitmqctl set_policy AE "^orders$" '{"alternate-exchange":"orders.unrouted"}' --apply-to exchanges
```

</details>

| After | email | points | audit | parking | Log |
|---|---|---|---|---|---|
| start | 0 | 0 | 0 | 0 | |
| `evt_0001` (`order.canceled`) | 0 | 0 | 0 | 1 | |
| `evt_0002` (`order.canceled`, mandatory) | 0 | 0 | 0 | 2 | no `RETURNED` line |
| `evt_0003` (`order.created`) | 1 | 1 | 1 | 2 | |

**Why.** The policy makes `orders.unrouted` the alternate exchange of `orders`, with no change in the service and no redeploy. A message that matches no binding of `orders` is passed to it, and its fanout copies it to `unrouted.parking`. A message that an alternate exchange accepts counts as routed, so the mandatory flag has nothing to return. `order.created` is routed normally and never reaches the alternate exchange.

The catcher:

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/unroutable/Topology.java#parking -->
```java
static void declareParking(Channel channel) throws IOException {
    channel.exchangeDeclare(UNROUTED_EXCHANGE, BuiltinExchangeType.FANOUT, true);
    channel.queueDeclare(PARKING, true, false, false, null);
    channel.queueBind(PARKING, UNROUTED_EXCHANGE, "");
}
```
<!-- /snippet -->

## The rule

> **A message that matches no binding is dropped silently, unless you ask RabbitMQ to give it back.**

The rule of a direct exchange, written as code. RabbitMQ does the real routing; the project never calls this class to route anything:

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/unroutable/DirectRule.java#rule -->
```java
/** The queues that receive a copy: every queue with a binding whose key is EQUAL to the routing key. Maybe none. */
static Set<String> queuesFor(String routingKey, List<Binding> bindings) {
    Set<String> queues = new LinkedHashSet<>();
    for (Binding binding : bindings) {
        if (binding.key().equals(routingKey)) {
            queues.add(binding.queue());
        }
    }
    return queues;
}

/** An unroutable message: the exchange finds no queue for it. */
static boolean isUnroutable(String routingKey, List<Binding> bindings) {
    return queuesFor(routingKey, bindings).isEmpty();
}
```
<!-- /snippet -->

What the rule does **not** promise: that the fixes are a delivery guarantee (they cover one failure, *no binding matched*), that the broker took the message at all (publisher confirms answer that, in a later episode) or that a parking queue is read by anyone.

## Read the code

| File | Role |
|---|---|
| `App.java` | Entry point: one command per step of the experiments |
| `Topology.java` | The exchanges, the queue names, the table of bindings and the calls that declare them |
| `DirectRule.java` | The rule of the exchange written as code, so that you can read and test it |
| `Publisher.java` | The producer: publishes to `orders`, with or without the mandatory flag, and the return listener |
| `Setup.java` | Declares the starting point of each experiment |
| `Binding.java`, `OrderEvent.java`, `EventJson.java`, `Args.java`, `Log.java` | Small supporting pieces; you can skip them |

Follow the execution:

**1. The entry point** maps each command of the experiments to one class.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/unroutable/App.java#commands -->
```java
switch (args[0]) {
    case "setup" -> Setup.orders();
    case "setup-parking" -> Setup.ordersWithParking();
    case "publish" -> Publisher.publish(
            options.text("key", "order.canceled"),
            options.number("count", 1),
            options.number("first", 1),
            Boolean.parseBoolean(options.text("mandatory", "false")));
    case "explain" -> explain(options.text("key", "order.canceled"));
    case "reset" -> Topology.reset();
```
<!-- /snippet -->

**2. The publisher** registers the return listener only when the flag is set, and publishes with the flag as the third argument of `basicPublish`. (`deliveryMode(2)` and `messageId` are not needed to understand routing. There is no publisher confirm here: it has its own episode.)

**3. The listener** runs only for a message that matched no binding and was published with the flag.

**4. The catcher** is an ordinary fanout exchange with one queue. Declaring it connects it to nothing: the connection is the policy.

**5. The tests** need no broker.

<!-- snippet: src/test/java/com/fromdevtodev/rabbitmq/unroutable/UnroutableTest.java#test -->
```java
@Test
void aKeyWithABindingReachesEveryQueueBoundWithIt() {
    assertEquals(Set.of("notifications.email", "loyalty.points", "audit.log"), DirectRule.queuesFor("order.created", Topology.ORDERS));
    assertEquals(Set.of("notifications.email", "audit.log"), DirectRule.queuesFor("order.cancelled", Topology.ORDERS));
}

@Test
void oneLetterOfDifferenceMakesTheMessageUnroutable() {
    assertTrue(DirectRule.isUnroutable("order.canceled", Topology.ORDERS));      // one L: no binding has this key
    assertFalse(DirectRule.isUnroutable("order.cancelled", Topology.ORDERS));    // two Ls: bound
    assertTrue(DirectRule.isUnroutable("Order.created", Topology.ORDERS));       // keys are exact, case included
}
```
<!-- /snippet -->

## Try it yourself

1. **Fix the typo.** Publish `--key order.cancelled` (two Ls) with `./run.sh app publish`. Which queues get it? *Answer:* `notifications.email` and `audit.log`.
2. **A case you did not type.** Does `Order.created` (capital O) match `order.created`? *Answer:* no, keys are exact, case included. The unit test checks it.
3. **A second parking queue.** Bind another queue to `orders.unrouted` (fanout). *Answer:* both queues get a copy of every unroutable message, because a fanout copies to every bound queue.
4. **Without the policy.** Run experiment 3, then `./run.sh reset` and publish the typo again: where does it go? *Answer:* nowhere. The policy is cleared with the exchanges, and the message is dropped again.
5. **Compliance.** Compliance wants every unroutable event kept, and the order service must not be redeployed. Which fix? *Answer:* the alternate exchange with a queue behind it, attached by a policy. The flag is set in the producer's code.

## Where it fits

Use the mandatory flag when the **publisher** can do something with a returned message. Use an alternate exchange when you need to **keep** unroutable messages and look at them later. Both turn a silent failure into one you can see.

Neither is a delivery guarantee. They cover one failure: no binding matched. A parking queue that nobody watches only hides the problem in a new place, so alert when it is not empty. For an optional event, dropping may be exactly what you want.

## Things that surprise people

- **A publish call that returns normally says very little.** It does not say that a queue received the message.
- **The return arrives after the call.** It is a callback, not an exception, so a publisher that closes its connection at once may never see it.
- **The flag does not store anything.** The returned message is in the publisher's hands.
- **An alternate exchange counts as routing.** A mandatory message that the alternate exchange accepts is not returned.
- **A policy outlives the exchange it applied to.** `./run.sh reset` clears it.
- **Keys are exact.** One letter, or one capital, is a different key.
- **Port conflicts.** Another episode's RabbitMQ uses the same ports. Run `./run.sh down` in that episode's folder first.

## Summary

- A message that matches no binding is an **unroutable message**; by default the exchange drops it and the publisher hears nothing.
- The **mandatory flag** makes the broker return it to the publisher, which handles it in a return listener.
- An **alternate exchange**, set by a policy, passes it to another exchange, and a queue behind it keeps it.
- Neither one is a delivery guarantee, and a parking queue needs someone watching it.

## What this project does not cover

- **Publisher confirms**: whether the broker accepted a message at all (a later episode).
- **Dead-letter exchanges**: what happens to messages that are rejected or expire (a later episode).
- **Persistence and durability** of the messages; the exchanges and queues here are durable, but the messages are not the subject.
- **Consumers.** Nothing reads the queues: the messages only wait, so that you can count them.
- **Security, clustering, monitoring and production hardening.**
