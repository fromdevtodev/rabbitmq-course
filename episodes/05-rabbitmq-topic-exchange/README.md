# 05 — Topic exchange: routing with patterns (`*` and `#`)

**Level:** Engineer  
**Episode:** Topic exchange: routing with patterns (`*` and `#`)  
**Language:** English  
**Code:** Java 21 · Maven · RabbitMQ Java client · Docker Compose  
**Requires:** [04 — Direct exchange](../04-rabbitmq-direct-exchange/README.md)  
**Reading time:** about 14 minutes, plus the experiments

## YouTube

The video link will be added here when the episode is published.

This README is the **written version of the lesson**. It tells the same story as the video, with more room for the reasoning and for the code. You can read it before the video, after it, or instead of it. Everything it describes can be run on your machine with `./run.sh`.

> **The one sentence of this episode**
>
> **A topic exchange matches routing keys against word patterns: star is exactly one word, hash is zero or more.**

> **Learning-project disclaimer:** nothing here audits anything or stores a real event. The messages only wait in the queues, so that you can count them. The terminal demos in the video run this project against a real RabbitMQ. The code is illustrative, not production-ready (see [What this project does not cover](#what-this-project-does-not-cover)).

## Learning objectives

After this episode you should be able to:

- Define **topic exchange**, **star wildcard** and **hash wildcard**.
- Given a list of patterns and a routing key, say which queues receive a copy, word by word.
- Explain why `orders.*` does not match `orders` but `orders.#` does.
- Explain why a star inside a word, as in `ord*`, is not a wildcard.
- Write the pattern for "every EU event, whatever the domain" for keys shaped `domain.region.event`.
- Read the code of the demos and say which call declares the topic exchange, which call carries a pattern and which call carries a plain key.

## Contents

1. [The problem: six bindings, then eight](#the-problem-six-bindings-then-eight)
2. [The vocabulary](#the-vocabulary)
3. [Three beliefs, one is true](#three-beliefs-one-is-true)
4. [Experiment 1: watch the words decide](#experiment-1-watch-the-words-decide)
5. [Experiment 2: star, hash and a star inside a word](#experiment-2-star-hash-and-a-star-inside-a-word)
6. [The rule](#the-rule)
7. [Read the code](#read-the-code)
8. [Try it yourself](#try-it-yourself), [where it fits](#where-it-fits), [things that surprise people](#things-that-surprise-people), [summary](#summary) and [what this project does not cover](#what-this-project-does-not-cover)

| Video scene | Where it is in this README |
|---|---|
| Cold open, "One binding per key, or two patterns" | [The problem](#the-problem-six-bindings-then-eight) |
| "Patterns match words", "A routing key is a path", "From a routing key to the queues" | [The vocabulary](#the-vocabulary) |
| "Three beliefs about patterns", "What people believe, and the rule" | [Three beliefs, one is true](#three-beliefs-one-is-true) |
| Challenge: which patterns match, board and demo "Watch the words decide" | [Experiment 1](#experiment-1-watch-the-words-decide) |
| Board and demo "Star, hash and a glob" | [Experiment 2](#experiment-2-star-hash-and-a-star-inside-a-word) |
| "Topic exchange: three calls", challenge: every EU event | [Read the code](#read-the-code), [Try it yourself](#try-it-yourself) |
| "Topic exchange: when it fits" | [Where it fits](#where-it-fits) |
| Recap | [Summary](#summary) |

## The problem: six bindings, then eight

Messages are published to one exchange with routing keys shaped `domain.region.event`: `orders.eu.created`, `payments.us.failed`. An audit queue wants **every payment event, in every region**, and **only the orders from the EU**.

With a direct exchange (episode 4) a binding matches only when its key is **equal** to the routing key, so audit needs one binding per key:

```text
payments.eu.created   payments.eu.failed
payments.us.created   payments.us.failed
orders.eu.created     orders.eu.cancelled
```

Six bindings for two regions, eight when a third region appears, and every new region or event means more bindings to write and to forget. What audit really wants is a **rule about the shape** of the key. A topic exchange lets you write it with two bindings:

```text
payments.#
orders.eu.*
```

## The vocabulary

Three terms, each tied to a place in the project.

- **Topic exchange.** An exchange that matches the routing key of a message against the **pattern** of each binding, and delivers a copy to every queue with a matching binding. In this project it is `events`, declared in `Topology.declareExchange` with `BuiltinExchangeType.TOPIC`.
- **Star wildcard** (`*`). Stands for **exactly one word**: any word, but one.
- **Hash wildcard** (`#`). Stands for **zero or more words**: none, one or several.

A routing key for a topic exchange is a list of **words separated by dots**: `orders.eu.created` has three words. Think of a path on disk: `docs/eu/invoices`. A word is the text between two dots. The wildcards live in the **bindings**; the message always carries a plain key.

The patterns used in the first experiment:

| Queue | Bound to `events` with the pattern |
|---|---|
| `created-orders` | `orders.*.created` |
| `all-orders` | `orders.#` |
| `eu-audit` | `*.eu.*` |
| `payments` | `payments.#` |

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/Topology.java#patterns -->
```java
static final List<Binding> ROUTING = List.of(
        new Binding(CREATED_ORDERS, "orders.*.created"),
        new Binding(ALL_ORDERS, "orders.#"),
        new Binding(EU_AUDIT, "*.eu.*"),
        new Binding(PAYMENTS, "payments.#"));
```
<!-- /snippet -->

## Three beliefs, one is true

People believe three things about patterns. Only one is true.

1. **"A star works inside a word: `ord*` matches `orders`."** False. A star is a wildcard only when it is a **whole word**. Inside a word it is just a character.
2. **"Star and hash are the same: both mean anything."** False. Star is **exactly one** word. Hash is **zero or more**.
3. **"Wildcards stand for whole words, between the dots."** True. The exchange compares the pattern with the key **word by word**.

The next two experiments test them with a real broker.

## Experiment 1: watch the words decide

**Predict first.** A message is published with the routing key `orders.eu.created`. Which of the four patterns above match it? Then `orders.us.cancelled`, then `payments.eu.failed`. Write down the counters you expect for the four queues before you run anything. (If you want to check your reasoning without a broker, `./run.sh app explain --key orders.eu.created` prints what the rule predicts. It is the rule written as code, not a measurement.)

```bash
./run.sh up           # RabbitMQ in Docker
./run.sh experiment1  # the whole experiment, step by step
```

<details>
<summary>The raw commands behind <code>./run.sh experiment1</code></summary>

```bash
./run.sh app setup                                  # the exchange events, four queues, four patterns
./run.sh bindings                                   # the bindings of events
./run.sh app publish --key orders.eu.created --first 1       # evt_0001
./run.sh app publish --key orders.us.cancelled --first 2     # evt_0002
./run.sh app publish --key payments.eu.failed --first 3      # evt_0003
./run.sh counters
```

</details>

What the experiment checks, step by step (ready messages in `created-orders`, `all-orders`, `eu-audit` and `payments`):

| After | created-orders | all-orders | eu-audit | payments |
|---|---|---|---|---|
| start | 0 | 0 | 0 | 0 |
| `evt_0001` (`orders.eu.created`) | 1 | 1 | 1 | 0 |
| `evt_0002` (`orders.us.cancelled`) | 1 | 2 | 1 | 0 |
| `evt_0003` (`payments.eu.failed`) | 1 | 2 | 2 | 1 |

**Why.**

- `orders.eu.created` fits `orders.*.created` (a star for `eu`), `orders.#` (the hash takes `eu.created`) and `*.eu.*` (a star for `orders` and a star for `created`). `payments.#` needs the first word to be `payments`.
- `orders.us.cancelled` fits only `orders.#`: the last word is not `created`, and the second word is not `eu`.
- `payments.eu.failed` fits `*.eu.*` and `payments.#`. The two patterns that start with `orders` do not move.

The producer was the same code in every step, and it never named a queue.

## Experiment 2: star, hash and a star inside a word

**Predict first.** Three queues are bound with `orders.*`, `orders.#` and `ord*.eu.created`. Three messages are published, with keys of one, two and three words: `orders`, `orders.eu` and `orders.eu.created`. How many messages will each queue hold at the end, and after each step?

```bash
./run.sh experiment2  # star against hash, step by step
```

<details>
<summary>The raw commands behind <code>./run.sh experiment2</code></summary>

```bash
./run.sh app setup-comparison                       # orders-star, orders-hash, orders-glob
./run.sh bindings
./run.sh app publish --key orders --first 1                  # evt_0001, one word
./run.sh app publish --key orders.eu --first 2               # evt_0002, two words
./run.sh app publish --key orders.eu.created --first 3       # evt_0003, three words
./run.sh counters
```

</details>

| After | orders-star | orders-hash | orders-glob |
|---|---|---|---|
| start | 0 | 0 | 0 |
| `evt_0001` (`orders`) | 0 | 1 | 0 |
| `evt_0002` (`orders.eu`) | 1 | 2 | 0 |
| `evt_0003` (`orders.eu.created`) | 1 | 3 | 0 |

**Why.**

- `orders` has **one** word. `orders.*` needs the word `orders` **and** exactly one more, so it does not match. `orders.#` lets the hash take zero words, so it matches.
- `orders.eu` has two words: now the star has exactly one word to stand for, and both patterns match.
- `orders.eu.created` has three words: too many for one star, still fine for a hash.
- `ord*.eu.created` never matches. Its first word is the text `ord*`, and no key of ours has that word. The star is a character there, not a wildcard.

## The rule

> **A topic exchange matches routing keys against word patterns: star is exactly one word, hash is zero or more.**

What the rule does **not** promise: that a message reaches any queue at all (a key that matches no pattern is discarded by default, as with a direct exchange), that matching works inside a word (it does not), or that every consumer gets every message regardless of the key (a fanout exchange does that, episode 6).

## Read the code

| File | Role |
|---|---|
| `App.java` | Entry point: one command per step of the experiments |
| `Topology.java` | The exchange, the queue names, the two tables of bindings and the calls that declare and bind |
| `TopicRule.java` | The rule of the exchange written as code, so that you can read and test it (RabbitMQ does the real routing) |
| `Publisher.java` | The producer: publishes to `events` with a plain routing key |
| `Setup.java` | Declares the starting point of each experiment |
| `Binding.java`, `DomainEvent.java`, `EventJson.java`, `Args.java`, `Log.java` | Small supporting pieces; you can skip them |

Follow the execution:

**1. The entry point** maps each command of the experiments to one class.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/App.java#commands -->
```java
switch (args[0]) {
    case "setup" -> Setup.routing();
    case "setup-comparison" -> Setup.comparison();
    case "publish" -> Publisher.publish(
            options.text("key", "orders.eu.created"),
            options.number("count", 1),
            options.number("first", 1));
    case "explain" -> explain(options.text("key", "orders.eu.created"), options.text("set", "routing"));
    case "reset" -> Topology.reset();
```
<!-- /snippet -->

**2. The exchange.** Everyone who touches `events` declares it first, so whoever starts first creates it.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/Topology.java#exchange -->
```java
static void declareExchange(Channel channel) throws IOException {
    channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
}
```
<!-- /snippet -->

`TOPIC` is the exchange type: a binding matches when its **pattern** fits the routing key, word by word. The `true` makes the exchange durable, which belongs to a later episode.

**3. The second table of bindings.** The comparison queues of experiment 2, including the star inside a word.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/Topology.java#comparison -->
```java
static final List<Binding> COMPARISON = List.of(
        new Binding(ORDERS_STAR, "orders.*"),
        new Binding(ORDERS_HASH, "orders.#"),
        new Binding(ORDERS_GLOB, "ord*.eu.created"));
```
<!-- /snippet -->

**4. Declaring and binding.** Only the third argument of `queueBind` changed meaning: it is now a pattern.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/Topology.java#bind -->
```java
static void declareAndBind(Channel channel, String queue, String pattern) throws IOException {
    channel.queueDeclare(queue, true, false, false, null);
    channel.queueBind(queue, EXCHANGE, pattern);
}
```
<!-- /snippet -->

**5. The publisher.** The line that matters is `basicPublish`: an exchange and a **plain** routing key. No wildcards in a message.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/Publisher.java#publish -->
```java
/** Publishes events {@code first .. first + count - 1} to the exchange events, all with the given routing key. */
static void publish(String routingKey, long count, long first) throws IOException, TimeoutException {
    try (Connection connection = Topology.connect("event-service");
         Channel channel = connection.createChannel()) {
        Topology.declareExchange(channel);
        for (long seq = first; seq < first + count; seq++) {
            DomainEvent event = DomainEvent.of(seq, routingKey);
            channel.basicPublish(Topology.EXCHANGE, routingKey, properties(event), EventJson.toBytes(event));
            Log.info("event-service", "published " + event.eventId() + " to '" + Topology.EXCHANGE
                    + "' with routing key " + routingKey);
        }
    }
}
```
<!-- /snippet -->

> **Ignore these for now.** `deliveryMode(2)` and `messageId` are not needed to understand routing: persistence belongs to the durability episode. There is no publisher confirm here either, and no `mandatory` flag: that is why a message with no matching pattern vanishes without an error. Both have their own episodes.

**6. The rule, and its tests.** `TopicRule` is the matching of the whole episode in a few lines: `*` consumes one word, `#` tries every number of words, including none. The tests need no broker.

<!-- snippet: src/main/java/com/fromdevtodev/rabbitmq/topic/TopicRule.java#rule -->
```java
/** Does the pattern match the key? Both are lists of words separated by dots. */
static boolean matches(String pattern, String key) {
    return matches(pattern.split("\\."), 0, key.split("\\."), 0);
}

private static boolean matches(String[] pattern, int p, String[] key, int k) {
    if (p == pattern.length) {
        return k == key.length;                      // the pattern is used up: the key must be used up too
    }
    if (pattern[p].equals("#")) {                    // hash: zero or more words
        for (int skip = k; skip <= key.length; skip++) {
            if (matches(pattern, p + 1, key, skip)) {
                return true;
            }
        }
        return false;
    }
    if (k == key.length) {
        return false;                                // the key has no word left for this part of the pattern
    }
    boolean sameWord = pattern[p].equals("*") || pattern[p].equals(key[k]);   // star: exactly one word
    return sameWord && matches(pattern, p + 1, key, k + 1);
}
```
<!-- /snippet -->

<!-- snippet: src/test/java/com/fromdevtodev/rabbitmq/topic/TopicExchangeTest.java#test -->
```java
@Test
void everyQueueWithAMatchingPatternGetsACopy() {
    assertEquals(Set.of("created-orders", "all-orders", "eu-audit"), TopicRule.queuesFor("orders.eu.created", Topology.ROUTING));
    assertEquals(Set.of("all-orders"), TopicRule.queuesFor("orders.us.cancelled", Topology.ROUTING));
    assertEquals(Set.of("eu-audit", "payments"), TopicRule.queuesFor("payments.eu.failed", Topology.ROUTING));
}

@Test
void starIsExactlyOneWordAndHashIsZeroOrMore() {
    assertFalse(TopicRule.matches("orders.*", "orders"));
    assertTrue(TopicRule.matches("orders.*", "orders.eu"));
    assertFalse(TopicRule.matches("orders.*", "orders.eu.created"));

    assertTrue(TopicRule.matches("orders.#", "orders"));
    assertTrue(TopicRule.matches("orders.#", "orders.eu"));
    assertTrue(TopicRule.matches("orders.#", "orders.eu.created"));
}

@Test
void aStarInsideAWordIsNotAWildcard() {
    assertFalse(TopicRule.matches("ord*.eu.created", "orders.eu.created"));
    assertFalse(TopicRule.matches("ord*", "orders"));
}

@Test
void aKeyThatMatchesNoPatternDeliversNothing() {
    assertTrue(TopicRule.queuesFor("shipments.us.created", Topology.ROUTING).isEmpty());
}
```
<!-- /snippet -->

The wildcard is recognised only when the **whole word** of the pattern is `*` or `#`; any other word must be equal.

## Try it yourself

1. **Every EU event.** Keys are `domain.region.event`. Which pattern gives a queue every EU event, whatever the domain? *Answer:* `*.eu.*`. `#.eu` would need `eu` to be the last word, and `eu.#` would need it to be the first.
2. **Audit with two patterns.** Audit wants every payment event and only the EU orders. Show that two bindings are enough. *Answer:* `payments.#` and `orders.eu.*`; the test below checks it for all six keys of the problem section, and for a new region.

   <!-- snippet: src/test/java/com/fromdevtodev/rabbitmq/topic/TopicExchangeTest.java#test-audit -->
```java
@Test
void directBindingsGrowWithEveryRegionButTwoPatternsStayTheSame() {
    List<String> twoRegions = auditKeys("eu", "us");
    List<String> threeRegions = auditKeys("eu", "us", "apac");
    assertEquals(6, twoRegions.size());     // one direct binding per key
    assertEquals(8, threeRegions.size());
    for (String key : threeRegions) {
        boolean covered = TopicRule.matches("payments.#", key) || TopicRule.matches("orders.eu.*", key);
        assertTrue(covered, key);           // the same two patterns still cover every key
    }
}

/** What audit wants: every payment event, whatever the region, and the EU orders. */
private static List<String> auditKeys(String... regions) {
    List<String> keys = new ArrayList<>();
    for (String region : regions) {
        keys.add("payments." + region + ".created");
        keys.add("payments." + region + ".failed");
    }
    keys.add("orders.eu.created");
    keys.add("orders.eu.cancelled");
    return keys;
}
```
<!-- /snippet -->

3. **A fifth queue.** Make a queue `shipping` that receives every event whose last word is `shipped`, for any domain and region. Which pattern? *Answer:* `#.shipped`. Add it to a copy of `Setup`, run it, publish `--key orders.eu.shipped` and look at `./run.sh counters`.
4. **Hash alone.** What does a queue bound with `#` receive? *Answer:* every message published to the exchange: zero or more words matches any key.
5. **Hash in the middle.** Does `orders.#.created` match `orders.created`? *Answer:* yes, the hash can stand for zero words. Check with `./run.sh app explain --key orders.created --set routing` after adding it to the table (it prints the rule's prediction, not a measurement).

## Where it fits

Use a topic exchange when your keys are **words with a structure** (`domain.region.event`), when each consumer wants a **slice** of the keys, and when a new region or event should not mean new bindings.

It is more than you need when exact keys are enough: a direct exchange is simpler to reason about. And the price of patterns is that the **order of the words becomes a contract**: renaming or reordering a word breaks every binding that depends on it. If every queue must get every message whatever the key, binding patterns is the wrong tool: another exchange broadcasts without looking at the key.

## Things that surprise people

- **`orders.*` does not match `orders`.** A star needs exactly one word.
- **Star is not a text match.** `ord*` is not "anything that starts with ord".
- **The wildcards belong to bindings.** A message published with a key containing `*` is just a key with a star in a word.
- **Several patterns can match the same key.** Each matching queue gets one copy; a queue with two matching patterns still gets one.
- **Keys are exact words.** `eu`, `EU` and `eu ` are three different words.
- **Port conflicts.** Another episode's RabbitMQ uses the same ports. Run `./run.sh down` in that episode's folder first.

## Summary

- A **topic exchange** matches the routing key against the **pattern** of each binding, word by word.
- **Star** is exactly one word; **hash** is zero or more. A star inside a word is just a character.
- The message carries a plain key; the wildcards live in the bindings.

**Next episode:** *Fanout exchange*: every queue gets every message, and the key does not matter.

## What this project does not cover

- How fanout or headers exchanges route (later episodes).
- What happens to a message that matches no pattern beyond "it is discarded by default" (episode 7): the `mandatory` flag, returns and alternate exchanges.
- Consuming: taking messages from these queues works as in episode 2, and acknowledgements come later.
- Durability, publisher confirms, queue types and any RabbitMQ version-specific behaviour.
- Production concerns: security, configuration and error handling are minimal on purpose.
