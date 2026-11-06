package com.fromdevtodev.rabbitmq.prefetch;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A worker that takes messages from the queue at its own pace and acknowledges each one when it is done.
 *
 * <p>Two things matter in this episode:
 * <ul>
 *   <li>{@code channel.basicQos(prefetch)} is the <em>prefetch</em>: the most unacknowledged messages RabbitMQ will
 *       give <em>this consumer</em> at once. {@code 0} means no limit.</li>
 *   <li>The delivery callback only <em>takes</em> the message and puts it in a local buffer (the consumer's "hands");
 *       a loop on the main thread works through the buffer at {@code perSecond}. That is how a real slow consumer
 *       behaves: messages RabbitMQ has handed over wait in memory until it gets to them, so you can see how many it
 *       is holding ("in flight").</li>
 * </ul>
 *
 * <p>The message is acknowledged only after the (simulated) work. If the process is stopped, whatever it still
 * holds is unacknowledged, and RabbitMQ puts it back in the queue (see the previous episode).
 */
final class Consumer {
    private Consumer() { }

    /** Runs until Ctrl+C. {@code reportMillis <= 0} turns the periodic report off. */
    static void run(String name, double perSecond, int prefetch, long reportMillis)
            throws IOException, TimeoutException, InterruptedException {
        RatePacer pacer = new RatePacer(perSecond);
        Tally tally = new Tally();
        BlockingQueue<Delivery> hands = new LinkedBlockingQueue<>();
        try (Connection connection = Topology.connect("consumer-" + name);
             Channel channel = connection.createChannel()) {
            Topology.declare(channel);
            channel.basicQos(prefetch);
            DeliverCallback onDelivery = (consumerTag, delivery) -> {
                tally.received();
                hands.add(delivery);
            };
            channel.basicConsume(Topology.QUEUE, false, onDelivery, consumerTag -> { });
            Log.info(name, "consuming '" + Topology.QUEUE + "' with prefetch "
                    + (prefetch == 0 ? "0 (no limit)" : prefetch) + " at "
                    + (perSecond > 0 ? perSecond + " per second" : "full speed") + " (Ctrl+C to stop)");
            startReporter(name, tally, reportMillis);

            while (!Thread.currentThread().isInterrupted()) {
                Delivery delivery = hands.poll(200, TimeUnit.MILLISECONDS);
                if (delivery == null) {
                    continue;
                }
                pacer.pace();
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                tally.acknowledged();
            }
        }
    }

    private static void startReporter(String name, Tally tally, long reportMillis) {
        if (reportMillis <= 0) {
            return;
        }
        Thread reporter = new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(reportMillis);
                    if (tally.receivedCount() > 0) {
                        System.out.println(ReportLine.format(LocalTime.now(), name, tally.inFlight(), tally.handled()));
                    }
                }
            } catch (InterruptedException stop) {
                Thread.currentThread().interrupt();
            }
        }, "report-" + name);
        reporter.setDaemon(true);
        reporter.start();
    }
}
