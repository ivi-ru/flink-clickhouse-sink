package ru.ivi.opensource.flinkclickhousesink.applied;

public class UnexceptionableSink<T> implements Sink<T> {
    private final ClickHouseSinkBuffer clickHouseSinkBuffer;

    public UnexceptionableSink(ClickHouseSinkBuffer buffer) {
        this.clickHouseSinkBuffer = buffer;
    }

    @Override
    public void put(T message) {
        clickHouseSinkBuffer.put((String) message);
    }

    @Override
    public void close() {
        if (clickHouseSinkBuffer != null) {
            clickHouseSinkBuffer.close();
        }
    }
}
