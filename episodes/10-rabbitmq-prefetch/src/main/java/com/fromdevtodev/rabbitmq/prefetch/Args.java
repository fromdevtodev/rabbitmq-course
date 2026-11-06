package com.fromdevtodev.rabbitmq.prefetch;

import java.util.HashMap;
import java.util.Map;

/** Minimal {@code --name value} parser, so the project needs no CLI library. */
final class Args {
    private final Map<String, String> values = new HashMap<>();

    Args(String[] args, int from) {
        for (int i = from; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("expected '--name value', got: " + args[i]);
            }
            values.put(args[i].substring(2), args[++i]);
        }
    }

    long number(String name, long defaultValue) {
        return values.containsKey(name) ? Long.parseLong(values.get(name)) : defaultValue;
    }

    String text(String name, String defaultValue) {
        return values.getOrDefault(name, defaultValue);
    }
}
