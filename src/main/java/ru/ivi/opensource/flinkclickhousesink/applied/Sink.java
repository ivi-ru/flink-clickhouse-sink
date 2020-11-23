package ru.ivi.opensource.flinkclickhousesink.applied;

import java.util.concurrent.ExecutionException;

public interface Sink extends AutoCloseable {
    void put(String message) throws ExecutionException, InterruptedException;
}
