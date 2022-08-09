package ru.ivi.opensource.flinkclickhousesink.model;

public final class ClickHouseSinkConst {
    private ClickHouseSinkConst() {
    }

    public static final String TARGET_TABLE_NAME = "clickhouse.sink.target-table";
    public static final String MAX_BUFFER_SIZE = "clickhouse.sink.max-buffer-size";

    public static final String NUM_WRITERS = "clickhouse.sink.num-writers";
    public static final String QUEUE_MAX_CAPACITY = "clickhouse.sink.queue-max-capacity";
    public static final String TIMEOUT_SEC = "clickhouse.sink.timeout-sec";
    public static final String NUM_RETRIES = "clickhouse.sink.retries";
    public static final String FAILED_RECORDS_PATH = "clickhouse.sink.failed-records-path";
    public static final String IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED = "clickhouse.sink.ignoring-clickhouse-sending-exception-enabled";
    public static final String ENABLE_DUMPING_FAILED_RECORDS_TO_S3 = "clickhouse.sink.enable-dumping-failed-records-to-s3";
    public static final String AWS_ACCESS_KEY_ID = "clickhouse.sink.aws-access-key-id";
    public static final String AWS_SECRET_ACCESS_KEY = "clickhouse.sink.aws-secret-access-key";
    public static final String AWS_REGION = "clickhouse.sink.aws-region";
    public static final String AWS_S3_URL = "clickhouse.sink.aws-s3-url";
    public static final String AWS_S3_BUCKET = "clickhouse.sink.aws-s3-bucket";
}
