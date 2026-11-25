package com.fromdevtodev.rabbitmq.confirms;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The producer's memory of what it is still waiting for: publish sequence number to event.
 *
 * <p>The broker answers with a sequence number, and an answer may cover <em>several</em> messages at once ({@code multiple}: this
 * number and every lower one). Answers can also arrive in a different order than the publishes. So the producer cannot just count:
 * it keeps one entry per message and removes it when an answer covers it. Whatever is left in the map has no answer yet.
 *
 * <p>No RabbitMQ classes here: the callbacks of the client call {@link #confirmed} and {@link #nacked}, and the tests call them
 * without a broker. The client calls them from its own thread, so the map is concurrent.
 */
final class ConfirmTracker {

    /** The two answers the broker can give. No answer at all is not an {@code Answer}: it is a future that never completes. */
    enum Answer { CONFIRMED, NACKED }

    /** One message that an answer covered. */
    record Resolved(long sequence, OrderEvent event, Answer answer) { }

    private record Entry(OrderEvent event, CompletableFuture<Answer> answer) { }

    private final ConcurrentNavigableMap<Long, Entry> pending = new ConcurrentSkipListMap<>();

    // region readme:tracker
    /** Call it BEFORE publishing, so that an answer can never arrive for a message nobody remembers. */
    CompletableFuture<Answer> register(long sequence, OrderEvent event) {
        CompletableFuture<Answer> answer = new CompletableFuture<>();
        pending.put(sequence, new Entry(event, answer));
        return answer;
    }

    /** A basic.ack from the broker: the messages it covers are confirmed. */
    List<Resolved> confirmed(long sequence, boolean multiple) {
        return resolve(sequence, multiple, Answer.CONFIRMED);
    }

    /** A basic.nack from the broker: the messages it covers were NOT taken. */
    List<Resolved> nacked(long sequence, boolean multiple) {
        return resolve(sequence, multiple, Answer.NACKED);
    }

    private List<Resolved> resolve(long sequence, boolean multiple, Answer answer) {
        // multiple = true: this sequence number and every lower one; false: this one only.
        NavigableMap<Long, Entry> covered = multiple
                ? pending.headMap(sequence, true)
                : pending.subMap(sequence, true, sequence, true);
        List<Resolved> resolved = new ArrayList<>();
        for (Long number : new ArrayList<>(covered.keySet())) {
            Entry entry = pending.remove(number);
            if (entry != null) {   // an answer that covers a number twice must not resolve it twice
                entry.answer().complete(answer);
                resolved.add(new Resolved(number, entry.event(), answer));
            }
        }
        return resolved;
    }
    // endregion

    /** Messages that no answer has covered yet: their outcome is unknown. */
    List<Long> unanswered() {
        return new ArrayList<>(pending.keySet());
    }
}
