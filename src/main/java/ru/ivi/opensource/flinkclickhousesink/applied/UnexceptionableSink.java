package ru.ivi.opensource.flinkclickhousesink.applied;

public class UnexceptionableSink implements Sink {
    private final ClickHouseSinkBuffer clickHouseSinkBuffer;

    public UnexceptionableSink(ClickHouseSinkBuffer buffer) {
        this.clickHouseSinkBuffer = buffer;
    }

    @Override
    public void put(String message) {
        clickHouseSinkBuffer.put(message);
    }

    @Override
    public void close() {
        if (clickHouseSinkBuffer != null) {
            clickHouseSinkBuffer.close();
        }
    }
}
