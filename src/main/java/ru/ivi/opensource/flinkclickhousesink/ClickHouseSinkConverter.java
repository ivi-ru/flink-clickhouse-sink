package ru.ivi.opensource.flinkclickhousesink;

@FunctionalInterface
public interface ClickHouseSinkConverter<T> {
    String convert(T record);
}
