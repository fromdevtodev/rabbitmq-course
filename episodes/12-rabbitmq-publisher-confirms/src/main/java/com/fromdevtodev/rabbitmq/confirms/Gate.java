package com.fromdevtodev.rabbitmq.confirms;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A hook for scripts: before a message goes out, wait until a file exists, then delete it. A script that wants to act between two
 * messages (the experiments freeze the broker there) creates the file when it is ready, and the next message is released. Without
 * {@code --gate} nothing waits.
 */
final class Gate {
    private final Path file;

    Gate(Path file) {
        this.file = file;
    }

    /** A gate that never waits. */
    static Gate open() {
        return new Gate(null);
    }

    void await() throws IOException, InterruptedException {
        if (file == null) {
            return;
        }
        long deadline = System.currentTimeMillis() + 120_000;
        while (!Files.exists(file)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("the file " + file + " never appeared");
            }
            Thread.sleep(100);
        }
        Files.deleteIfExists(file);
    }
}
