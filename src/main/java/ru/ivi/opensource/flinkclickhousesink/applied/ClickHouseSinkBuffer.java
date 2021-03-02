package ru.ivi.opensource.flinkclickhousesink.applied;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseRequestBlank;
import ru.ivi.opensource.flinkclickhousesink.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ClickHouseSinkBuffer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSinkBuffer.class);

    private final ClickHouseWriter writer;
    private final String targetTable;
    private final int maxFlushBufferSize;
    private final long timeoutMillis;
    private final List<String> localValues;
    private final List<CompletableFuture<Boolean>> futures;

    private volatile long lastAddTimeMillis = System.currentTimeMillis();

    private ClickHouseSinkBuffer(
            ClickHouseWriter chWriter,
            long timeout,
            int maxBuffer,
            String table,
            List<CompletableFuture<Boolean>> futures
    ) {
        writer = chWriter;
        localValues = new ArrayList<>();
        timeoutMillis = timeout;
        maxFlushBufferSize = maxBuffer;
        targetTable = table;

        this.futures = futures;

        logger.info("Instance ClickHouse Sink, target table = {}, buffer size = {}", this.targetTable, this.maxFlushBufferSize);
    }

    String getTargetTable() {
        return targetTable;
    }

    public void put(String recordAsCSV) {
        tryAddToQueue();
        localValues.add(recordAsCSV);
    }

    synchronized void tryAddToQueue() {
        if (flushCondition()) {
            addToQueue();
            lastAddTimeMillis = System.currentTimeMillis();
        }
    }

    private void addToQueue() {
        List<String> deepCopy = buildDeepCopy(localValues);
        ClickHouseRequestBlank params = ClickHouseRequestBlank.Builder
                .aBuilder()
                .withValues(deepCopy)
                .withTargetTable(targetTable)
                .build();

        logger.debug("Build blank with params: buffer size = {}, target table  = {}", params.getValues().size(), params.getTargetTable());
        writer.put(params);

        localValues.clear();
    }

    private boolean flushCondition() {
        return localValues.size() > 0 && (checkSize() || checkTime());
    }

    private boolean checkSize() {
        return localValues.size() >= maxFlushBufferSize;
    }

    private boolean checkTime() {
        long current = System.currentTimeMillis();
        return current - lastAddTimeMillis > timeoutMillis;
    }

    private static List<String> buildDeepCopy(List<String> original) {
        return Collections.unmodifiableList(new ArrayList<>(original));
    }

    public void assertFuturesNotFailedYet() throws ExecutionException, InterruptedException {
        CompletableFuture<Void> future = FutureUtil.allOf(futures);
        //nonblocking operation
        if (future.isCompletedExceptionally()) {
            future.get();
        }
    }

    @Override
    public void close() {
        logger.info("ClickHouse sink buffer is shutting down.");
        if (localValues != null && localValues.size() > 0) {
            addToQueue();
        }
        logger.info("ClickHouse sink buffer shutdown complete.");
    }

    public static final class Builder {
        private String targetTable;
        private int maxFlushBufferSize;
        private int timeoutSec;
        private List<CompletableFuture<Boolean>> futures;

        private Builder() {
        }

        public static Builder aClickHouseSinkBuffer() {
            return new Builder();
        }

        public Builder withTargetTable(String targetTable) {
            this.targetTable = targetTable;
            return this;
        }

        public Builder withMaxFlushBufferSize(int maxFlushBufferSize) {
            this.maxFlushBufferSize = maxFlushBufferSize;
            return this;
        }

        public Builder withTimeoutSec(int timeoutSec) {
            this.timeoutSec = timeoutSec;
            return this;
        }

        public Builder withFutures(List<CompletableFuture<Boolean>> futures) {
            this.futures = futures;
            return this;
        }

        public ClickHouseSinkBuffer build(ClickHouseWriter writer) {

            Preconditions.checkNotNull(targetTable);
            Preconditions.checkArgument(maxFlushBufferSize > 0);
            Preconditions.checkArgument(timeoutSec > 0);

            return new ClickHouseSinkBuffer(
                    writer,
                    TimeUnit.SECONDS.toMillis(this.timeoutSec),
                    this.maxFlushBufferSize,
                    this.targetTable,
                    this.futures
            );
        }
    }
}
