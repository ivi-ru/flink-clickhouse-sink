package ru.ivi.opensource.flinkclickhousesink.applied;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class UnexceptionableSinkTest {

    private Sink unexceptionableSink;
    private ClickHouseSinkBuffer buffer;

    @Before
    public void setUp() {
        buffer = Mockito.mock(ClickHouseSinkBuffer.class);
        unexceptionableSink = new UnexceptionableSink(buffer);
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void put() throws ExecutionException, InterruptedException {
        String actual = "csv";

        doAnswer(invocation -> {
            String expected = (String) invocation.getArguments()[0];
            assertEquals(expected, actual);
            return invocation;
        }).when(buffer).put(Mockito.anyString());

        unexceptionableSink.put(actual);

        verify(buffer, times(0)).assertFuturesNotFailedYet();
    }
}