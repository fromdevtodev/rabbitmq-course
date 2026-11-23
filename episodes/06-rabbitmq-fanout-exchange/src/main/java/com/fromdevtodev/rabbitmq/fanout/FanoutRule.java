package com.fromdevtodev.rabbitmq.fanout;

import java.util.List;

/**
 * The routing rule of a fanout exchange, written as plain Java so that the unit tests can state it without a broker.
 * The broker does not run this code: it is the rule in executable form.
 */
final class FanoutRule {
    private FanoutRule() { }

    // region readme:rule
    /**
     * The queues that receive a copy of a message: every queue bound to the exchange. The routing key of the message is
     * accepted and ignored, and so are the binding keys. (Each queue of this project is bound once; what the broker does with
     * one queue bound twice is not covered here.)
     */
    static List<String> queuesFor(String routingKey, List<Binding> bindings) {
        return bindings.stream().map(Binding::queue).toList();
    }
    // endregion
}
