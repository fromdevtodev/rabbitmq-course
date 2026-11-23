package com.fromdevtodev.rabbitmq.unroutable;

/** One binding of the exchange: the queue and the binding key it is bound with. */
record Binding(String queue, String key) { }
