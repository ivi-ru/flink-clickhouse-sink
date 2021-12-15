package ru.ivi.opensource.flinkclickhousesink.applied;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ExceptionsThrowableSinkTest {

    private Sink exceptionsThrowableSink;
    private ClickHouseSinkBuffer buffer;

    @Before
    public void setUp() {
        buffer = Mockito.mock(ClickHouseSinkBuffer.class);
        exceptionsThrowableSink = new ExceptionsThrowableSink(buffer);
        MockitoAnnotations.initMocks(this);
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
            assertThat(expectedException.getMessage(), CoreMatchers.containsString("test Exception message"));
        }
        verify(buffer, times(1)).assertFuturesNotFailedYet();
    }
}