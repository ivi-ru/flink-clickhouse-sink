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
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;
import ru.ivi.opensource.flinkclickhousesink.util.ConfigUtil;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ClickHouseSinkScheduledCheckerAndCleanerTest {

    private ClickHouseSinkScheduledCheckerAndCleaner checker;
    private AtomicInteger counter = new AtomicInteger(0);
    private List<CompletableFuture<Boolean>> futures = Collections.synchronizedList(new LinkedList<>());

    @Before
    public void setUp() {
        Config config = ConfigFactory.load();
        Map<String, String> params = ConfigUtil.toMap(config);
        params.put(ClickHouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        params.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:8123");
        params.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "true");

        ClickHouseSinkCommonParams commonParams = new ClickHouseSinkCommonParams(params);
        checker = new ClickHouseSinkScheduledCheckerAndCleaner(commonParams, futures);

        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        checker.close();
    }

    @Test
    public void addSinkBuffer() {
        test(3, 3);
    }

    private void test(int numBuffers, int target) {
        for (int i = 0; i < numBuffers; i++) {
            ClickHouseSinkBuffer buffer = buildMockBuffer();
            checker.addSinkBuffer(buffer);
        }

        await()
                .atMost(2000, MILLISECONDS)
                .with()
                .pollInterval(200, MILLISECONDS)
                .until(() -> {
                    System.out.println(counter.get());
                    return counter.get() == target;
                });
    }

    private ClickHouseSinkBuffer buildMockBuffer() {
        ClickHouseSinkBuffer buffer = Mockito.mock(ClickHouseSinkBuffer.class);
        Mockito.doAnswer(invocationOnMock -> {
            counter.incrementAndGet();
            return invocationOnMock;
        }).when(buffer).tryAddToQueue();
        return buffer;
    }

    @Test
    public void checkBuffersAfterAttempt() {
        int first = 4;
        int second = 1;
        test(first, first);
        test(second, first * 2 + second);
    }
}