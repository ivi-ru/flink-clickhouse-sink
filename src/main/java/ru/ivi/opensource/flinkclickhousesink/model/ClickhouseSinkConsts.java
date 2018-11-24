package ru.ivi.opensource.flinkclickhousesink.model;

public final class ClickhouseSinkConsts {
    private ClickhouseSinkConsts() {
    }

    public static final String TARGET_TABLE_NAME = "clickhouse.sink.target-table";
    public static final String MAX_BUFFER_SIZE = "clickhouse.sink.max-buffer-size";

    public static final String NUM_WRITERS = "clickhouse.sink.num-writers";
    public static final String QUEUE_MAX_CAPACITY = "clickhouse.sink.queue-max-capacity";
    public static final String TIMEOUT_SEC = "clickhouse.sink.timeout-sec";
    public static final String NUM_RETRIES = "clickhouse.sink.retries";
    public static final String FAILED_RECORDS_PATH = "clickhouse.sink.failed-records-path";
}
