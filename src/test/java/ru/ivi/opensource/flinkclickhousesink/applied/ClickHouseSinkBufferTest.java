package ru.ivi.opensource.flinkclickhousesink.applied;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;
import ru.ivi.opensource.flinkclickhousesink.util.ConfigUtil;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ClickHouseSinkBufferTest {

    private static final int NUM_ATTEMPTS = 100;
    private static final int TIMEOUT_SEC = 2;

    private ClickHouseSinkBuffer buffer;
    private ClickHouseWriter writer;
    private ClickHouseSinkScheduledCheckerAndCleaner checker;
    private List<CompletableFuture<Boolean>> futures = Collections.synchronizedList(new LinkedList<>());

    @Before
    public void setUp() {
        writer = Mockito.mock(ClickHouseWriter.class);
        buffer = ClickHouseSinkBuffer.Builder
                .aClickHouseSinkBuffer()
                .withTargetTable("table")
                .withMaxFlushBufferSize(NUM_ATTEMPTS)
                .withTimeoutSec(TIMEOUT_SEC)
                .withFutures(futures)
                .build(writer);

        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void simplePut() throws ExecutionException, InterruptedException {
        Mockito.doAnswer(invocationOnMock -> {
            ClickHouseRequestBlank blank = invocationOnMock.getArgument(0);
            System.out.println(blank);
            assertEquals(NUM_ATTEMPTS, blank.getValues().size());
            assertEquals("table", blank.getTargetTable());
            return invocationOnMock;
        }).when(writer).put(Mockito.any());

        for (int i = 0; i < 100; i++) {
            buffer.put("csv");
        }
    }

    private void initChecker() {
        Config config = ConfigFactory.load();
        Map<String, String> params = ConfigUtil.toMap(config);
        params.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        params.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:8123");
        params.put(ClickHouseSinkConst.TIMEOUT_SEC, String.valueOf(TIMEOUT_SEC));
        params.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "true");

        ClickHouseSinkCommonParams commonParams = new ClickHouseSinkCommonParams(params);
        checker = new ClickHouseSinkScheduledCheckerAndCleaner(commonParams, futures);
    }

    @Test
    public void putWithCheck() throws ExecutionException, InterruptedException {
        initChecker();
        checker.addSinkBuffer(buffer);

        AtomicBoolean flag = new AtomicBoolean();
        Mockito.doAnswer(invocationOnMock -> {
            ClickHouseRequestBlank blank = invocationOnMock.getArgument(0);

            assertTrue(NUM_ATTEMPTS > blank.getValues().size());
            assertEquals("table", blank.getTargetTable());
            flag.set(true);
            return invocationOnMock;
        }).when(writer).put(Mockito.any());

        for (int i = 0; i < 80; i++) {
            buffer.put("csv");
        }

        await()
                .atMost(5000, MILLISECONDS)
                .with()
                .pollInterval(200, MILLISECONDS)
                .until(flag::get);
    }
}