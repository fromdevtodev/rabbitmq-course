package com.fromdevtodev.rabbitmq.unroutable;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The rule of a direct exchange, written as code so that you can read it. RabbitMQ implements this rule itself: the project never
 * calls this class to route anything. It exists so that you can predict the result of a publish and then check your prediction
 * against the real broker ({@code ./run.sh app explain --key order.canceled}, then {@code ./run.sh experiment1}).
 */
final class DirectRule {
    private DirectRule() { }

    // region readme:rule
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
    // endregion
}
