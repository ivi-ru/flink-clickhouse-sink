package ru.ivi.opensource.flinkclickhousesink.applied;

import java.util.concurrent.ExecutionException;

public interface Sink<T> extends AutoCloseable {
    void put(T message) throws ExecutionException, InterruptedException;
}
