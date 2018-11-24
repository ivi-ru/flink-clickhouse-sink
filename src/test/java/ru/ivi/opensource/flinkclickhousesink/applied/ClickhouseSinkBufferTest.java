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
import ru.ivi.opensource.flinkclickhousesink.applied.ClickhouseSinkBuffer;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickhouseSinkScheduledChecker;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickhouseWriter;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkCommonParams;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;
import ru.ivi.opensource.flinkclickhousesink.util.ConfigUtil;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ClickhouseSinkBufferTest {

    private static final int NUM_ATTEMPTS = 100;
    private static final int TIMEOUT_SEC = 2;

    private ClickhouseSinkBuffer buffer;
    private ClickhouseWriter writer;
    private ClickhouseSinkScheduledChecker checker;

    @Before
    public void setUp() throws Exception {
        writer = Mockito.mock(ClickhouseWriter.class);
        buffer = ClickhouseSinkBuffer.Builder
                .aClickhouseSinkBuffer()
                .withTargetTable("table")
                .withMaxFlushBufferSize(NUM_ATTEMPTS)
                .withTimeoutSec(TIMEOUT_SEC)
                .build(writer);

        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void simplePut() {
        Mockito.doAnswer(invocationOnMock -> {
            ClickhouseRequestBlank blank = invocationOnMock.getArgument(0);
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
        params.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "");
        params.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "");
        params.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "http://localhost:8123");
        params.put(ClickhouseSinkConsts.TIMEOUT_SEC, String.valueOf(TIMEOUT_SEC));

        ClickhouseSinkCommonParams commonParams = new ClickhouseSinkCommonParams(params);
        checker = new ClickhouseSinkScheduledChecker(commonParams);
    }

    @Test
    public void putWithCheck() {
        initChecker();
        checker.addSinkBuffer(buffer);

        AtomicBoolean flag = new AtomicBoolean();
        Mockito.doAnswer(invocationOnMock -> {
            ClickhouseRequestBlank blank = invocationOnMock.getArgument(0);

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