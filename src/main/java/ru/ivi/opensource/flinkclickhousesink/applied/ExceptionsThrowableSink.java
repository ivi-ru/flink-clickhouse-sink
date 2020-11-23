package ru.ivi.opensource.flinkclickhousesink.applied;

import java.util.concurrent.ExecutionException;

public class ExceptionsThrowableSink implements Sink {
    private final ClickHouseSinkBuffer clickHouseSinkBuffer;

    public ExceptionsThrowableSink(ClickHouseSinkBuffer buffer) {
        this.clickHouseSinkBuffer = buffer;
    }

    @Override
    public void put(String message) throws ExecutionException, InterruptedException {
        clickHouseSinkBuffer.put(message);
        clickHouseSinkBuffer.assertFuturesNotFailedYet();
    }

    @Override
    public void close() {
        if (clickHouseSinkBuffer != null) {
            clickHouseSinkBuffer.close();
        }
    }
}
