package com.fromdevtodev.rabbitmq.nack;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

/** One line per event, with a timestamp. */
final class Log {
    private Log() { }

    static void info(String who, String message) {
        System.out.printf("%s [%s] %s%n", LocalTime.now().truncatedTo(ChronoUnit.MILLIS), who, message);
    }
}
