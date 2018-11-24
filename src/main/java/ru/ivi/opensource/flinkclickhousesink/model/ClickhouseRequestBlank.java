package ru.ivi.opensource.flinkclickhousesink.model;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ClickhouseRequestBlank {
    private final List<String> values;
    private final String targetTable;
    //TODO think about it: change to int
    private final AtomicInteger attemptCounter;

    public ClickhouseRequestBlank(List<String> values, String targetTable) {
        this.values = values;
        this.targetTable = targetTable;
        this.attemptCounter = new AtomicInteger(0);
    }

    public List<String> getValues() {
        return values;
    }

    public AtomicInteger getAttemptCounter() {
        return attemptCounter;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public static final class Builder {
        private List<String> values;
        private String targetTable;

        private Builder() {
        }

        public static Builder aBuilder() {
            return new Builder();
        }

        public Builder withValues(List<String> values) {
            this.values = values;
            return this;
        }

        public Builder withTargetTable(String targetTable) {
            this.targetTable = targetTable;
            return this;
        }

        public ClickhouseRequestBlank build() {
            return new ClickhouseRequestBlank(values, targetTable);
        }
    }

    @Override
    public String toString() {
        return "ClickhouseRequestBlank{" +
                "values=" + values +
                ", targetTable='" + targetTable + '\'' +
                ", attemptCounter=" + attemptCounter +
                '}';
    }
}
