package ru.ivi.opensource.flinkclickhousesink;

import java.io.Serializable;

@FunctionalInterface
public interface ClickHouseSinkConverter<T> extends Serializable {
    String convert(T record);
}
