package ru.ivi.opensource.flinkclickhousesink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseSinkManager;
import ru.ivi.opensource.flinkclickhousesink.applied.Sink;

import java.util.Map;
import java.util.Properties;

public class ClickHouseSink<T> extends RichSinkFunction<T> {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSink.class);

    private static final Object DUMMY_LOCK = new Object();

    private final Properties localProperties;
    private final ClickHouseSinkConverter<T> clickHouseSinkConverter;

    private volatile static transient ClickHouseSinkManager sinkManager;
    private transient Sink sink;

    public ClickHouseSink(Properties properties,
                          ClickHouseSinkConverter<T> clickHouseSinkConverter) {
        this.localProperties = properties;
        this.clickHouseSinkConverter = clickHouseSinkConverter;
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
     * Add a record to sink
     *
     * @param record  record, which will be converted to csv, using {@link ClickHouseSinkConverter}
     * @param context ctx
     */
    @Override
    public void invoke(T record, Context context) {
        try {
            String recordAsCSV = clickHouseSinkConverter.convert(record);
            sink.put(recordAsCSV);
        } catch (Exception e) {
            logger.error("Error while sending data to ClickHouse, record = {}", record, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (sink != null) {
            sink.close();
        }

        if (sinkManager != null && !sinkManager.isClosed()) {
            synchronized (DUMMY_LOCK) {
                if (sinkManager != null && !sinkManager.isClosed()) {
                    sinkManager.close();
                    sinkManager = null;
                }
            }
        }

        super.close();
    }
}
