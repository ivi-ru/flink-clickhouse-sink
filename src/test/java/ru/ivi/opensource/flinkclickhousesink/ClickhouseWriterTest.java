package ru.ivi.opensource.flinkclickhousesink;

import com.google.common.util.concurrent.Striped;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.jmock.lib.concurrent.Blitzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.ClickHouseContainer;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickhouseSinkBuffer;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickhouseSinkManager;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.util.ConfigUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.awaitility.Awaitility.await;

public class ClickhouseWriterTest {

    private static final int MAX_ATTEMPT = 2;
    private static final String JDBC_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final String DB_URL = "http://hadoop-spark-linx-1.vcp.digitalaccess.ru:8123";
    private static final String PASS = "LrRNalY82qjLecn2";
    private static final String USER = "flink";
    private static final int HTTP_CLICKHOUSE_PORT = 8123;

    private AsyncHttpClient asyncHttpClient;
    private HikariDataSource hikariDataSource;
    private Map<String, String> params;

    @Rule
    public ClickHouseContainer clickhouse = new ClickHouseContainer();
    private ClickhouseSinkManager sinkManager;

    @Before
    public void setUp() throws Exception {
        Config config = ConfigFactory.load();
        params = ConfigUtil.toMap(config);

        params.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        int dockerActualPort = clickhouse.getMappedPort(HTTP_CLICKHOUSE_PORT);
        // для каждого теста поднимается новый контейнер с новым портом, поэтому его нужно каждый раз заново пробрасывать
        params.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:" + String.valueOf(dockerActualPort));

        asyncHttpClient = asyncHttpClient();

        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(clickhouse.getJdbcUrl());
        hikariConfig.setUsername(clickhouse.getUsername());
        hikariConfig.setPassword(clickhouse.getPassword());
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

        sinkManager = new ClickhouseSinkManager(params);
    }

    @After
    public void down() throws Exception{
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

    private void send(String data, String url, String basicCredentials, AtomicInteger counter) {
        String query = String.format("INSERT INTO %s VALUES %s ", "groot3.events", data);
        BoundRequestBuilder requestBuilder = asyncHttpClient.preparePost(url).setHeader("Authorization", basicCredentials);
        requestBuilder.setBody(query);
        ListenableFuture<Response> whenResponse = asyncHttpClient.executeRequest(requestBuilder.build());
        Runnable callback = () -> {
            try {
                Response response = whenResponse.get();
                System.out.println(Thread.currentThread().getName() + " " + response);

                if (response.getStatusCode() != 200) {
                    System.out.println(Thread.currentThread().getName() + " try to retry...");
                    int attempt = counter.incrementAndGet();
                    if (attempt > MAX_ATTEMPT) {
                        System.out.println("Failed ");
                    } else {
                        send(data, url, basicCredentials, counter);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };

        Executor executor = ForkJoinPool.commonPool();
        whenResponse.addListener(callback, executor);
    }

    @Test
    public void highConcurrentTest() throws Exception {

        final int numBuffers = 4;
        Striped<Lock> striped = Striped.lock(numBuffers);

        List<ClickhouseSinkBuffer> buffers = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 4; i++) {
            String targetTable = null;
            if (i % 2 != 0) {
                targetTable = "test.test0";
            } else targetTable = "test.test1";

            int maxBuffer = random.nextInt(1_000, 100_000);
            ClickhouseSinkBuffer clickhouseSinkBuffer = sinkManager.buildBuffer(targetTable, maxBuffer);
            buffers.add(clickhouseSinkBuffer);
        }

        final int attempts = 2_000_000;
        final int numThreads = numBuffers;
        Blitzer blitzer = new Blitzer(attempts, numThreads);
        blitzer.blitz(() -> {
            int id = ThreadLocalRandom.current().nextInt(0, numBuffers);
            Lock lock = striped.get(id);
            lock.lock();
            try {
                ClickhouseSinkBuffer sinkBuffer = buffers.get(id);
                String csv = null;
                if (id % 2 != 0) {
                    csv = "(10, 'title', 'container', 'drm', 'quality')";
                } else {
                    csv = "(11, 'title', 111)";
                }
                sinkBuffer.put(csv);
            } finally {
                lock.unlock();
            }
        });

        await()
                .atMost(3000, MILLISECONDS)
                .with()
                .pollInterval(500, MILLISECONDS)
                .until(() -> getCount("test.test0") + getCount("test.test1") == attempts);
    }
}