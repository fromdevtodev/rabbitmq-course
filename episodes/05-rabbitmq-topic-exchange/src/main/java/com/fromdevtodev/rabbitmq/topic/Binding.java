package com.fromdevtodev.rabbitmq.topic;

/** One binding of the exchange: the queue and the pattern (the binding key) it is bound with. */
record Binding(String queue, String pattern) { }
