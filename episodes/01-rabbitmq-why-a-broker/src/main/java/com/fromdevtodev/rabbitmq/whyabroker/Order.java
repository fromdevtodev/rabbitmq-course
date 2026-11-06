package com.fromdevtodev.rabbitmq.whyabroker;

/** An order that has already been saved. The ID looks like {@code ord_1042}. */
public record Order(String id, String customerId) { }
