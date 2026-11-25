package com.fromdevtodev.rabbitmq.confirms;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The second experiment: a message that gets no answer.
 *
 * <p>The producer publishes {@code evt_0001} and waits for its confirm. Then it waits at the gate (the experiment script
 * freezes the broker in between and releases it), and publishes {@code evt_0002} as sequence number 2. The broker is frozen,
 * so no answer comes. After the timeout the producer does the only thing that can make sure the broker ends up with the message:
 * it publishes {@code evt_0002} again, with the same event ID, as sequence number 3. When the broker wakes up it answers both.
 */
final class Republish {
    private Republish() { }

    static void run(Gate gate, long timeoutMs, long settleMs) throws IOException, TimeoutException, InterruptedException {
        try (Connection connection = Topology.connect(Publisher.WHO);
             Channel channel = connection.createChannel()) {
            Topology.requireQueue(channel);
            ConfirmTracker tracker = new ConfirmTracker();
            Publisher.listen(channel, tracker);

            Publisher.Sent first = Publisher.send(channel, tracker, OrderEvent.of(1));
            Publisher.awaitAnswer(first, 30_000);
            gate.await();   // the experiment script freezes the broker now, then releases the second message

            // region readme:republish
            Publisher.Sent second = Publisher.send(channel, tracker, OrderEvent.of(2));       // evt_0002 as #2
            Publisher.Sent third = null;
            if (Publisher.awaitAnswer(second, timeoutMs) == null) {
                // Silence. It is not a nack and it is not a loss: nobody knows. Publish the SAME event again.
                Log.info(Publisher.WHO, "publishing " + second.event().eventId() + " again (same event ID)");
                third = Publisher.send(channel, tracker, OrderEvent.of(2));                   // evt_0002 again, as #3
            }
            // endregion

            List<Publisher.Sent> all = third == null ? List.of(first, second) : List.of(first, second, third);
            all.forEach(sent -> Publisher.awaitAnswer(sent, settleMs));
            Publisher.summarise(all);
            long events = all.stream().map(sent -> sent.event().eventId()).distinct().count();
            Log.info(Publisher.WHO, "done: " + all.size() + " publishes for " + events + " events");
        }
    }
}
