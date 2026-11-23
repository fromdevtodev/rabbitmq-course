package com.fromdevtodev.rabbitmq.unroutable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/** The message body is just bytes to the broker; here the bytes are UTF-8 JSON. */
final class EventJson {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EventJson() { }

    static byte[] toBytes(OrderEvent event) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(event);
    }

    static OrderEvent fromBytes(byte[] body) throws IOException {
        return MAPPER.readValue(body, OrderEvent.class);
    }
}
