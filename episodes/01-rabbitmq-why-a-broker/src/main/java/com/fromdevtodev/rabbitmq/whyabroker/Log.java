package com.fromdevtodev.rabbitmq.whyabroker;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

/** One line per event, with a timestamp, so you can read the timing in the terminal. */
final class Log {
    private Log() { }

    static void info(String who, String message) {
        System.out.printf("%s [%s] %s%n", LocalTime.now().truncatedTo(ChronoUnit.MILLIS), who, message);
    }
}
