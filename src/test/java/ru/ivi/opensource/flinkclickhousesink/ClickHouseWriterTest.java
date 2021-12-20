package ru.ivi.opensource.flinkclickhousesink;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.hamcrest.CoreMatchers;
import org.jmock.lib.concurrent.Blitzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseSinkManager;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseWriter;
import ru.ivi.opensource.flinkclickhousesink.applied.Sink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;
import ru.ivi.opensource.flinkclickhousesink.util.ConfigUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ClickHouseWriterTest {

    private static final int HTTP_CLICKHOUSE_PORT = 8123;

    private static final String JDBC_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    private HikariDataSource hikariDataSource;

    @Rule
    public ClickHouseContainer clickHouse = new ClickHouseContainer();

    private ClickHouseSinkManager sinkManager;

    private Map<String, String> params;
    private ClickHouseSinkCommonParams clickHouseSinkCommonParams;

    @Before
    public void setUp() throws Exception {
        Config config = ConfigFactory.load();
        params = ConfigUtil.toMap(config);

        params.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        int dockerActualPort = clickHouse.getMappedPort(HTTP_CLICKHOUSE_PORT);

        // container with CH is raised for every test -> we should config host and port every time
        params.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://" + clickHouse.getContainerIpAddress() + ":" + dockerActualPort);
        params.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "true");
        clickHouseSinkCommonParams = new ClickHouseSinkCommonParams(params);

        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(clickHouse.getJdbcUrl());
        hikariConfig.setUsername(clickHouse.getUsername());
        hikariConfig.setPassword(clickHouse.getPassword());
        hikariDataSource = new HikariDataSource(hikariConfig);

        try (Connection connection = hikariDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeQuery("CREATE DATABASE IF NOT EXISTS test;");

            statement.executeQuery("DROP TABLE IF EXISTS test.test0;");
            statement.executeQuery("CREATE TABLE test.test0 (" +
                    "id UInt64," +
                    "title String," +
                    "container String," +
                    "drm String," +
                    "quality String)" +
                    "ENGINE = Log;");

            statement.executeQuery("DROP TABLE IF EXISTS test.test1;");
            statement.executeQuery("CREATE TABLE test.test1 (" +
                    "id UInt64," +
                    "title String," +
                    "num UInt64)" +
                    "ENGINE = Log;");
        }

        sinkManager = new ClickHouseSinkManager(params);
    }

    @After
    public void down() throws Exception {
        sinkManager.close();
    }

    private int getCount(String tableName) throws Exception {
        int res = 0;
        try (Connection connection = hikariDataSource.getConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select count (*) as total from " + tableName);
            while (resultSet.next()) {
                int count = resultSet.getInt("total");
                System.out.println("Count " + tableName + " = " + count);
                res = count;
            }
        }
        return res;
    }

    @Test
    public void highConcurrentTest() throws Exception {

        final int numBuffers = 4;
        Striped<Lock> striped = Striped.lock(numBuffers);

        List<Sink> buffers = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 4; i++) {
            String targetTable;
            if (i % 2 != 0) {
                targetTable = "test.test0";
            } else targetTable = "test.test1";

            int maxBuffer = random.nextInt(1_000, 100_000);
            Sink sink = sinkManager.buildSink(targetTable, maxBuffer);
            buffers.add(sink);
        }

        final int attempts = 2_000_000;
        Blitzer blitzer = new Blitzer(attempts, numBuffers);
        blitzer.blitz(() -> {
            int id = ThreadLocalRandom.current().nextInt(0, numBuffers);
            Lock lock = striped.get(id);
            lock.lock();
            try {
                Sink sink = buffers.get(id);
                String csv;
                if (id % 2 != 0) {
                    csv = "(10, 'title', 'container', 'drm', 'quality')";
                } else {
                    csv = "(11, 'title', 111)";
                }
                sink.put(csv);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        });

        await()
                .atMost(10000, MILLISECONDS)
                .with()
                .pollInterval(500, MILLISECONDS)
                .until(() -> getCount("test.test0") + getCount("test.test1") == attempts);
    }

    @Test
    public void testInvalidRequestException() throws Exception {
        ClickHouseWriter clickHouseWriter = new ClickHouseWriter(clickHouseSinkCommonParams,
                Collections.synchronizedList(new LinkedList<>()));
        clickHouseWriter.put(ClickHouseRequestBlank.Builder
                .aBuilder()
                .withValues(ImmutableList.of("('10')"))
                .withTargetTable("test.test0")
                .build());

        try {
            clickHouseWriter.close();
            fail("Expected RuntimeException.");
        } catch (RuntimeException expected) {
        }
    }

    @Test
    public void testWaitLastRequestSuccess() throws Exception {
        AsyncHttpClient asyncHttpClient = Mockito.spy(Dsl.asyncHttpClient());

        ClickHouseWriter clickHouseWriter = new ClickHouseWriter(clickHouseSinkCommonParams,
                Collections.synchronizedList(Lists.newLinkedList()), asyncHttpClient);
        clickHouseWriter.put(ClickHouseRequestBlank.Builder
                .aBuilder()
                .withValues(ImmutableList.of("(10, 'title', 'container', 'drm', 'quality')"))
                .withTargetTable("test.test0")
                .build());

        clickHouseWriter.close();

        await()
                .atMost(10000, MILLISECONDS)
                .with()
                .pollInterval(500, MILLISECONDS)
                .until(() -> getCount("test.test0") == 1);

        Mockito.verify(asyncHttpClient, Mockito.times(1))
                .executeRequest(Mockito.any(Request.class));
    }

    @Test
    public void testMaxRetries() throws Exception {
        int maxRetries = clickHouseSinkCommonParams.getMaxRetries();
        AsyncHttpClient asyncHttpClient = Mockito.spy(Dsl.asyncHttpClient());

        ClickHouseWriter clickHouseWriter = new ClickHouseWriter(clickHouseSinkCommonParams,
                Collections.synchronizedList(Lists.newLinkedList()),
                asyncHttpClient);
        clickHouseWriter.put(ClickHouseRequestBlank.Builder
                .aBuilder()
                .withValues(ImmutableList.of("('10')"))
                .withTargetTable("test.test0")
                .build());

        try {
            clickHouseWriter.close();
            fail("Expected RuntimeException.");
        } catch (RuntimeException expected) {
        }

        Mockito.verify(asyncHttpClient, Mockito.times(maxRetries + 1))
                .executeRequest(Mockito.any(Request.class));
    }

    @Test
    public void testExceptionInCallback() throws Exception {
        int maxRetries = clickHouseSinkCommonParams.getMaxRetries();
        AsyncHttpClient asyncHttpClient = Mockito.spy(Dsl.asyncHttpClient());

        Mockito.when(asyncHttpClient.executeRequest(Mockito.any(Request.class)))
                .thenReturn(new ListenableFuture.CompletedFailure<>(new NullPointerException("NPE")));

        ClickHouseWriter clickHouseWriter = new ClickHouseWriter(clickHouseSinkCommonParams,
                Collections.synchronizedList(Lists.newLinkedList()),
                asyncHttpClient);
        clickHouseWriter.put(ClickHouseRequestBlank.Builder.aBuilder()
                .withValues(ImmutableList.of("(10, 'title', 'container', 'drm', 'quality')"))
                .withTargetTable("test.test0")
                .build());

        try {
            clickHouseWriter.close();
            fail("Expected RuntimeException.");
        } catch (RuntimeException expected) {
            assertThat(expected.getMessage(), CoreMatchers.containsString("NPE"));
        }

        Mockito.verify(asyncHttpClient, Mockito.times(maxRetries + 1)).executeRequest(Mockito.any(Request.class));
    }

    @Test
    public void flinkPipelineTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        ParameterTool parameters = ParameterTool.fromMap(params);
        env.getConfig().setGlobalJobParameters(parameters);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().disableSysoutLogging();

        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "test.test1");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "100");

        List<Record> l = new ArrayList<>();
        l.add(new Record(10, "super-movie-0", 100));
        l.add(new Record(20, "super-movie-1", 200));
        l.add(new Record(30, "super-movie-2", 300));
        int size = l.size();

        ClickHouseSinkConverter<Record> clickHouseSinkConverter = record -> "(" +
                record.id +
                ", " +
                "'" +
                record.title +
                "', " +
                record.num +
                ")";

        env.fromCollection(l)
                .addSink(new ClickHouseSink<>(props, clickHouseSinkConverter));

        env.execute("Flink test");

        await()
                .atMost(10000, MILLISECONDS)
                .with()
                .pollInterval(500, MILLISECONDS)
                .until(() -> getCount("test.test1") == size);
    }

    // for test.test1 table
    private static class Record {
        final long id;
        final String title;
        final long num;

        private Record(long id, String title, long num) {
            this.id = id;
            this.title = title;
            this.num = num;
        }
    }
}