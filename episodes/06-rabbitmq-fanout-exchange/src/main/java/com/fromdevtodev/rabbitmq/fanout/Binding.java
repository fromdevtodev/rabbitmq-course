package com.fromdevtodev.rabbitmq.fanout;

/** One binding of the exchange: the queue and the binding key it is bound with. On a fanout exchange the key has no effect. */
record Binding(String queue, String key) { }
