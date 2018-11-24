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
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.util.ConfigUtil;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ClickhouseSinkScheduledCheckerTest {

    private ClickhouseSinkScheduledChecker checker;
    private AtomicInteger counter = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        Config config = ConfigFactory.load();
        Map<String, String> params = ConfigUtil.toMap(config);
        params.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        params.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:8123");

        ClickhouseSinkCommonParams commonParams = new ClickhouseSinkCommonParams(params);
        checker = new ClickhouseSinkScheduledChecker(commonParams);

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
            ClickhouseSinkBuffer buffer = buildMockBuffer();
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

    private ClickhouseSinkBuffer buildMockBuffer() {
        ClickhouseSinkBuffer buffer = Mockito.mock(ClickhouseSinkBuffer.class);
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