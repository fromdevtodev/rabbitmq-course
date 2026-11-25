package com.fromdevtodev.rabbitmq.confirms;

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

    /** A true/false option: {@code --persistent true}. Anything other than "true" (any case) is false. */
    boolean flag(String name, boolean defaultValue) {
        return values.containsKey(name) ? Boolean.parseBoolean(values.get(name)) : defaultValue;
    }

    String text(String name, String defaultValue) {
        return values.getOrDefault(name, defaultValue);
    }

    /** Whether the option was given at all. */
    boolean has(String name) {
        return values.containsKey(name);
    }

    /** A value the command cannot run without. */
    String required(String name) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required option --" + name);
        }
        return value;
    }
}
