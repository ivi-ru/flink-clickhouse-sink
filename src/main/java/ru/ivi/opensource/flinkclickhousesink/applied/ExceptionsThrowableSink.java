package ru.ivi.opensource.flinkclickhousesink.applied;

import java.util.concurrent.ExecutionException;

public class ExceptionsThrowableSink<T> implements Sink<T> {
    private final ClickHouseSinkBuffer clickHouseSinkBuffer;

    public ExceptionsThrowableSink(ClickHouseSinkBuffer buffer) {
        this.clickHouseSinkBuffer = buffer;
    }

    @Override
    public void put(T message) throws ExecutionException, InterruptedException {
        clickHouseSinkBuffer.put((String) message);
        clickHouseSinkBuffer.assertFuturesNotFailedYet();
    }

    @Override
    public void close() {
        if (clickHouseSinkBuffer != null) {
            clickHouseSinkBuffer.close();
        }
    }
}
