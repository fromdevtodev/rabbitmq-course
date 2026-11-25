package com.fromdevtodev.rabbitmq.confirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The producer: it publishes order events to {@code orders.in} with publisher confirms switched on, and logs every answer.
 *
 * <p>Every message is persistent, as in the durability episode. Publishing is the easy part: the call returns at once and says
 * nothing about the broker. What the broker thinks of the message arrives later, as a separate answer, and this class matches
 * each answer to its message through the publish sequence number.
 *
 * <p>There is no consumer in this project: no message is ever read, and that is part of the point.
 */
final class Publisher {
    static final String WHO = "order-service";

    /** {@code ONE_BY_ONE}: wait for the answer before the next publish. {@code BURST}: publish everything, then collect the answers. */
    enum Pace { ONE_BY_ONE, BURST }

    /** One publish: its sequence number, its event, and the future of its answer. */
    record Sent(long sequence, OrderEvent event, CompletableFuture<ConfirmTracker.Answer> answer) { }

    private Publisher() { }

    /** Publishes events {@code first .. first + count - 1}, in that order, and reports what the broker said. */
    static void publish(long count, long first, Pace pace, long timeoutMs, Gate gate)
            throws IOException, TimeoutException, InterruptedException {
        try (Connection connection = Topology.connect(WHO);
             Channel channel = connection.createChannel()) {
            Topology.requireQueue(channel);
            ConfirmTracker tracker = new ConfirmTracker();
            listen(channel, tracker);
            List<Sent> all = new ArrayList<>();
            for (long number = first; number < first + count; number++) {
                if (number > first) {
                    gate.await();   // a script may hold the next message back; without --gate this returns at once
                }
                Sent sent = send(channel, tracker, OrderEvent.of(number));
                all.add(sent);
                if (pace == Pace.ONE_BY_ONE) {
                    awaitAnswer(sent, timeoutMs);
                }
            }
            if (pace == Pace.BURST) {
                all.forEach(sent -> awaitAnswer(sent, timeoutMs));
            }
            summarise(all);
        }
    }

    // region readme:listener
    /** Confirm mode is a setting of the channel: switch it on once, then listen for the two kinds of answer. */
    static void listen(Channel channel, ConfirmTracker tracker) throws IOException {
        channel.confirmSelect();
        channel.addConfirmListener(
                (seq, multiple) -> report(tracker.confirmed(seq, multiple)),   // basic.ack
                (seq, multiple) -> report(tracker.nacked(seq, multiple)));     // basic.nack
    }
    // endregion

    // region readme:publish
    /** Publishes one event and returns at once; the answer arrives later, through the listener. */
    static Sent send(Channel channel, ConfirmTracker tracker, OrderEvent event) throws IOException {
        long seq = channel.getNextPublishSeqNo();                           // the number this message will get
        var answer = tracker.register(seq, event);                          // remember it BEFORE it goes out
        channel.basicPublish("", Topology.ORDERS_IN, MessageProperties.PERSISTENT_TEXT_PLAIN, EventJson.toBytes(event));
        Log.info(WHO, "published " + event.eventId() + " as #" + seq + " to " + Topology.ORDERS_IN + ", persistent");
        return new Sent(seq, event, answer);
    }
    // endregion

    // region readme:await
    /** The answer, or {@code null} when none arrives in time: that is not a failure, it is an unknown outcome. */
    static ConfirmTracker.Answer awaitAnswer(Sent sent, long timeoutMs) {
        try {
            return sent.answer().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            Log.info(WHO, "no answer for #" + sent.sequence() + " (" + sent.event().eventId() + ") after " + timeoutMs
                    + " ms: the outcome is unknown");
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }
    // endregion

    private static void report(List<ConfirmTracker.Resolved> resolved) {
        for (ConfirmTracker.Resolved r : resolved) {
            String what = r.answer() == ConfirmTracker.Answer.CONFIRMED ? "confirmed" : "NACKED by the broker";
            Log.info(WHO, "#" + r.sequence() + " " + what + " (" + r.event().eventId() + ")");
        }
    }

    static void summarise(List<Sent> all) {
        long confirmed = all.stream().filter(s -> s.answer().isDone() && s.answer().getNow(null) == ConfirmTracker.Answer.CONFIRMED).count();
        long nacked = all.stream().filter(s -> s.answer().isDone() && s.answer().getNow(null) == ConfirmTracker.Answer.NACKED).count();
        long silent = all.size() - confirmed - nacked;
        Log.info(WHO, "answers: " + confirmed + " confirmed, " + nacked + " nacked, " + silent + " without an answer");
    }
}
