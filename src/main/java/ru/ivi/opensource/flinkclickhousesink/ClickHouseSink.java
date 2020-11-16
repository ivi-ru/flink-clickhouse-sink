package ru.ivi.opensource.flinkclickhousesink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseSinkManager;
import ru.ivi.opensource.flinkclickhousesink.applied.Sink;

import java.util.Map;
import java.util.Properties;


public class ClickHouseSink extends RichSinkFunction<String> {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSink.class);

    private static final Object DUMMY_LOCK = new Object();

    private final Properties localProperties;

    private volatile static transient ClickHouseSinkManager sinkManager;
    private transient Sink<String> sink;

    public ClickHouseSink(Properties properties) {
        this.localProperties = properties;
    }

    @Override
    public void open(Configuration config) {
        if (sinkManager == null) {
            synchronized (DUMMY_LOCK) {
                if (sinkManager == null) {
                    Map<String, String> params = getRuntimeContext()
                            .getExecutionConfig()
                            .getGlobalJobParameters()
                            .toMap();

                    sinkManager = new ClickHouseSinkManager(params);
                }
            }
        }

        sink = sinkManager.buildSink(localProperties);
    }

    /**
     * Add csv to sink
     *
     * @param recordAsCSV csv-event
     */
    @Override
    public void invoke(String recordAsCSV, Context context) {
        try {
            sink.put(recordAsCSV);
        } catch (Exception e) {
            logger.error("Error while sending data to ClickHouse, record = {}", recordAsCSV, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (sink != null) {
            sink.close();
        }

        if (sinkManager != null) {
            if (!sinkManager.isClosed()) {
                synchronized (DUMMY_LOCK) {
                    if (!sinkManager.isClosed()) {
                        sinkManager.close();
                    }
                }
            }
        }

        super.close();
    }
}
