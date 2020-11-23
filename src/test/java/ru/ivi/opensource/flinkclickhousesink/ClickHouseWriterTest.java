package ru.ivi.opensource.flinkclickhousesink;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.asynchttpclient.*;
import org.hamcrest.CoreMatchers;
import org.jmock.lib.concurrent.Blitzer;
import org.junit.*;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.fail;

public class ClickHouseWriterTest {

    private static final int MAX_ATTEMPT = 2;
    private static final String JDBC_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    private static final int HTTP_CLICKHOUSE_PORT = 8123;

    private AsyncHttpClient asyncHttpClient;
    private HikariDataSource hikariDataSource;

    @Rule
    public ClickHouseContainer clickHouse = new ClickHouseContainer();

    private ClickHouseSinkManager sinkManager;

    private ClickHouseSinkCommonParams clickHouseSinkCommonParams;

    @Before
    public void setUp() throws Exception {
        Config config = ConfigFactory.load();
        Map<String, String> params = ConfigUtil.toMap(config);

        params.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        int dockerActualPort = clickHouse.getMappedPort(HTTP_CLICKHOUSE_PORT);

        // container with CH is raised for every test -> we should config host and port every time
        params.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://" + clickHouse.getContainerIpAddress() + ":" + dockerActualPort);
        params.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "true");
        clickHouseSinkCommonParams = new ClickHouseSinkCommonParams(params);

        asyncHttpClient = asyncHttpClient();

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
        final int numThreads = numBuffers;
        Blitzer blitzer = new Blitzer(attempts, numThreads);
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
            Assert.assertThat(expected.getMessage(), CoreMatchers.containsString("NPE"));
        }

        Mockito.verify(asyncHttpClient, Mockito.times(maxRetries + 1)).executeRequest(Mockito.any(Request.class));
    }
}