package ru.ivi.opensource.flinkclickhousesink.applied;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.OngoingStubbing;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ExceptionsThrowableSinkTest {

    private Sink exceptionsThrowableSink;
    private ClickHouseSinkBuffer buffer;

    @Before
    public void setUp() throws Exception {
        buffer = Mockito.mock(ClickHouseSinkBuffer.class);
        exceptionsThrowableSink = new ExceptionsThrowableSink(buffer);
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void putWithException() throws ExecutionException, InterruptedException {
        String actual = "csv";

        doThrow(new InterruptedException("test Exception message"))
                .when(buffer)
                .assertFuturesNotFailedYet();
        doAnswer(invocation -> {
            String expected = (String) invocation.getArguments()[0];
            assertEquals(expected, actual);
            return invocation;
        }).when(buffer).put(Mockito.anyString());

        try {
            exceptionsThrowableSink.put(actual);
        } catch (InterruptedException expectedException) {
            Assert.assertThat(expectedException.getMessage(), CoreMatchers.containsString("test Exception message"));
        }
        verify(buffer, times(1)).assertFuturesNotFailedYet();
    }
}