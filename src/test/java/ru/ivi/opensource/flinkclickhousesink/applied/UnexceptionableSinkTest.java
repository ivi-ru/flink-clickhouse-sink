package ru.ivi.opensource.flinkclickhousesink.applied;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class UnexceptionableSinkTest {

    private Sink unexceptionableSink;
    private ClickHouseSinkBuffer buffer;

    @Before
    public void setUp() throws Exception {
        buffer = Mockito.mock(ClickHouseSinkBuffer.class);
        unexceptionableSink = new UnexceptionableSink(buffer);
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
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