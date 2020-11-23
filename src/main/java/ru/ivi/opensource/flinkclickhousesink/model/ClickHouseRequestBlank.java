package ru.ivi.opensource.flinkclickhousesink.model;

import java.util.List;

public class ClickHouseRequestBlank {
    private final List<String> values;
    private final String targetTable;
    private int attemptCounter;

    private Exception exception;

    public ClickHouseRequestBlank(List<String> values, String targetTable, Exception exception) {
        this.values = values;
        this.targetTable = targetTable;
        this.attemptCounter = 0;
        this.exception = exception;
    }

    public List<String> getValues() {
        return values;
    }

    public void incrementCounter() {
        this.attemptCounter++;
    }

    public int getAttemptCounter() {
        return attemptCounter;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public static final class Builder {
        private List<String> values;
        private String targetTable;
        private Exception exception;

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

        public Builder withException(Exception exception) {
            this.exception = exception;
            return this;
        }

        public ClickHouseRequestBlank build() {
            return new ClickHouseRequestBlank(values, targetTable, exception);
        }
    }

    @Override
    public String toString() {
        return "ClickHouseRequestBlank{" +
                "values=" + values +
                ", targetTable='" + targetTable + '\'' +
                ", attemptCounter=" + attemptCounter +
                ", exception=" + exception +
                '}';
    }
}
