package ru.ivi.opensource.flinkclickhousesink.model;

import com.google.common.base.Preconditions;

import java.util.Map;

import static ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst.FAILED_RECORDS_PATH;
import static ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED;
import static ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst.NUM_RETRIES;
import static ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst.NUM_WRITERS;
import static ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst.QUEUE_MAX_CAPACITY;
import static ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst.TIMEOUT_SEC;

public class ClickHouseSinkCommonParams {

    private final ClickHouseClusterSettings clickHouseClusterSettings;

    private final String failedRecordsPath;
    private final int numWriters;
    private final int queueMaxCapacity;
    private final boolean ignoringClickHouseSendingExceptionEnabled;
    private final boolean enableDumpingFailedRecordsToS3;
    private final int timeout;
    private final int maxRetries;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String awsS3Region;
    private final String awsS3Url;
    private final String awsS3Bucket;

    public ClickHouseSinkCommonParams(Map<String, String> params) {
        Preconditions.checkNotNull(params.get(IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED),
                "Parameter " + IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED + " must be initialized");

        this.clickHouseClusterSettings = new ClickHouseClusterSettings(params);
        this.numWriters = Integer.parseInt(params.get(NUM_WRITERS));
        this.queueMaxCapacity = Integer.parseInt(params.get(QUEUE_MAX_CAPACITY));
        this.maxRetries = Integer.parseInt(params.get(NUM_RETRIES));
        this.timeout = Integer.parseInt(params.get(TIMEOUT_SEC));
        this.failedRecordsPath = params.get(FAILED_RECORDS_PATH);
        this.ignoringClickHouseSendingExceptionEnabled = Boolean.parseBoolean(params.get(IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED));
        this.enableDumpingFailedRecordsToS3 = Boolean.parseBoolean(params.get(ENABLE_DUMPING_FAILED_RECORDS_TO_S3));
        this.awsAccessKeyId = params.get(AWS_ACCESS_KEY_ID);
        this.awsSecretAccessKey = params.get(AWS_SECRET_ACCESS_KEY);
        this.awsS3Region = params.get(AWS_REGION);
        this.awsS3Url = params.get(AWS_S3_URL);
        this.awsS3Bucket = params.get(AWS_S3_BUCKET);

        Preconditions.checkNotNull(failedRecordsPath);
        Preconditions.checkArgument(queueMaxCapacity > 0);
        Preconditions.checkArgument(numWriters > 0);
        Preconditions.checkArgument(timeout > 0);
        Preconditions.checkArgument(maxRetries > 0);
    }

    public int getNumWriters() {
        return numWriters;
    }

    public int getQueueMaxCapacity() {
        return queueMaxCapacity;
    }

    public boolean isIgnoringClickHouseSendingExceptionEnabled() {
        return ignoringClickHouseSendingExceptionEnabled;
    }

    public ClickHouseClusterSettings getClickHouseClusterSettings() {
        return clickHouseClusterSettings;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getFailedRecordsPath() {
        return failedRecordsPath;
    }

    public Boolean isS3Enabled() {
        return enableDumpingFailedRecordsToS3;
    }

    public String getAwsAccessKeyId() {
        return awsAccessKeyId;
    }

    public String getAwsSecretAccessKey() {
        return awsSecretAccessKey;
    }

    public String getAwsS3Region() {
        return awsS3Region;
    }

    public String getAwsS3Url() {
        return awsS3Url;
    }

    public String getAwsS3Bucket() {
        return awsS3Bucket;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "ClickHouseSinkCommonParams{" +
                "clickHouseClusterSettings=" + clickHouseClusterSettings +
                ", failedRecordsPath='" + failedRecordsPath + '\'' +
                ", numWriters=" + numWriters +
                ", queueMaxCapacity=" + queueMaxCapacity +
                ", ignoringClickHouseSendingExceptionEnabled=" + ignoringClickHouseSendingExceptionEnabled +
                ", enableDumpingFailedRecordsToS3=" + enableDumpingFailedRecordsToS3 +
                ", timeout=" + timeout +
                ", maxRetries=" + maxRetries +
                ", awsAccessKeyId='" + awsAccessKeyId + '\'' +
                ", awsSecretAccessKey='" + awsSecretAccessKey + '\'' +
                ", awsS3Region='" + awsS3Region + '\'' +
                ", awsS3Url='" + awsS3Url + '\'' +
                ", awsS3Bucket='" + awsS3Bucket + '\'' +
                '}';
    }
}
