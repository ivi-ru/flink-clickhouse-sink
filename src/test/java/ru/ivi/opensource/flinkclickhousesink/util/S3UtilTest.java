package ru.ivi.opensource.flinkclickhousesink.util;

import org.junit.Before;
import org.junit.Test;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;

import java.util.HashMap;
import java.util.Map;


public class S3UtilTest {

    private Map<String, String> settingsMap;

    @Before
    public void setUp() throws Exception {
        settingsMap = new HashMap<>();
        settingsMap.put("clickhouse.sink.target-table", "test.test_table");
        settingsMap.put("clickhouse.sink.max-buffer-size", "30000");
        settingsMap.put("clickhouse.sink.num-writers", "5");
        settingsMap.put("clickhouse.sink.queue-max-capacity", "30000");
        settingsMap.put("clickhouse.sink.timeout-sec", "10");
        settingsMap.put("clickhouse.sink.retries", "3");
        settingsMap.put("clickhouse.sink.failed-records-path", ".failed");
        settingsMap.put("clickhouse.sink.ignoring-clickhouse-sending-exception-enabled", "false");
        settingsMap.put("clickhouse.access.hosts", "1.1.1.1:8123");
        settingsMap.put("clickhouse.access.user", "test");
        settingsMap.put("clickhouse.access.password", "test");
        settingsMap.put("clickhouse.sink.enable-dumping-failed-records-to-s3", "true");
        settingsMap.put("clickhouse.sink.aws-access-key-id", "");
        settingsMap.put("clickhouse.sink.aws-secret-access-key", "");
        settingsMap.put("clickhouse.sink.aws-region", "");
        settingsMap.put("clickhouse.sink.aws-s3-url", "");
        settingsMap.put("clickhouse.sink.aws-s3-bucket", "");
    }

    @Test
    public void testSaveFailedRecordsToS3() throws Exception {
        String filePath = "/Users/rajatchaudhary/Downloads/sample_text.txt";

        ClickHouseSinkCommonParams sinkSettings = new ClickHouseSinkCommonParams(settingsMap);

        S3Util.saveFailedRecordsToS3(filePath, sinkSettings);
    }
}
