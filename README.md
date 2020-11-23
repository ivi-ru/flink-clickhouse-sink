
# Flink-ClickHouse-Sink

[![Build Status](https://travis-ci.com/ivi-ru/flink-clickhouse-sink.svg?branch=master)](https://travis-ci.com/ivi-ru/flink-clickhouse-sink)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ru.ivi.opensource/flink-clickhouse-sink/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ru.ivi.opensource/flink-clickhouse-sink/)

## Description

[Flink](https://github.com/apache/flink) sink for [ClickHouse](https://github.com/yandex/ClickHouse) database. 
Powered by [Async Http Client](https://github.com/AsyncHttpClient/async-http-client).

High-performance library for loading data to ClickHouse. 

It has two triggers for loading data:
_by timeout_ and _by buffer size_.

##### Version map
|flink    |flink-clickhouse-sink | 
|:-------:|:--------------------:| 
|1.3.*    |1.0.0                 |
|1.9.0    |1.1.0                 |
|1.9.0    |1.2.0                 |


### Install

##### Maven Central

```xml
<dependency>
  <groupId>ru.ivi.opensource</groupId>
  <artifactId>flink-clickhouse-sink</artifactId>
  <version>1.2.0</version>
</dependency>
```

## Usage
### Properties
The flink-clickhouse-sink uses two parts of configuration properties: 
common and for each sink in you operators chain.

**The common part** (use like global):

 `clickhouse.sink.num-writers` - number of writers, which build and  send requests, 
 
 `clickhouse.sink.queue-max-capacity` - max capacity (batches) of blank's queue,
 
 `clickhouse.sink.timeout-sec` - timeout for loading data,
 
 `clickhouse.sink.retries` - max number of retries,
 
 `clickhouse.sink.failed-records-path`- path for failed records,
 
 `clickhouse.sink.ignoring-clickhouse-sending-exception-enabled` - required boolean parameter responsible for raising (false) or not (true) ClickHouse sending exception in main thread. 
 if `ignoring-clickhouse-sending-exception-enabled` is true, exception while clickhouse sending is ignored and failed data automatically goes to the disk.
 if `ignoring-clickhouse-sending-exception-enabled` is false, clickhouse sending exception thrown in "main" thread (thread which called ClickhHouseSink::invoke) and data also goes to the disk.

**The sink part** (use in chain):

 `clickhouse.sink.target-table` - target table in ClickHouse,
 
 `clickhouse.sink.max-buffer-size`- buffer size.

### In code
The main thing: the clickhouse-sink works with events in string 
(ClickHouse insert format, like CSV) format.
You have to convert your event to csv format (like usual insert in database).

For example, you have event-pojo:
 ```java
class A {
    public final String str;
    public final int integer;
    
    public A(String str, int i){
        this.str = str;
        this.integer = i;
    }
}
```
You have to convert this pojo like this:
```java
public static String convertToCsv(A a) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    
    // add a.str
    builder.append("'");
    builder.append(a.str);
    builder.append("', ");
    
    // add a.intger
    builder.append(String.valueOf(a.integer));
    builder.append(" )");
    return builder.toString();
}
```
And then add record to sink.

You have to add global parameters for Flink environment:
```java
StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
Map<String, String> globalParameters = new HashMap<>();

// ClickHouse cluster properties
globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, ...);
globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, ...);
globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, ...);

// sink common
globalParameters.put(ClickHouseSinkConsts.TIMEOUT_SEC, ...);
globalParameters.put(ClickHouseSinkConsts.FAILED_RECORDS_PATH, ...);
globalParameters.put(ClickHouseSinkConsts.NUM_WRITERS, ...);
globalParameters.put(ClickHouseSinkConsts.NUM_RETRIES, ...);
globalParameters.put(ClickHouseSinkConsts.QUEUE_MAX_CAPACITY, ...);
globalParameters.put(ClickHouseSinkConsts.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, ...);

// set global paramaters
ParameterTool parameters = ParameterTool.fromMap(buildGlobalParameters(config));
environment.getConfig().setGlobalJobParameters(parameters);

```

And add your sink like this:
```java
// create converter
public YourEventConverter {
    String toClickHouseInsertFormat (YourEvent yourEvent){
        String chFormat = ...;
        ....
        return chFormat;
    }
}

// create props for sink
Properties props = new Properties();
props.put(ClickHouseSinkConsts.TARGET_TABLE_NAME, "your_table");
props.put(ClickHouseSinkConsts.MAX_BUFFER_SIZE, "10000");

// build chain
DataStream<YourEvent> dataStream = ...;
dataStream.map(YourEventConverter::toClickHouseInsertFormat)
          .name("convert YourEvent to ClickHouse table format")
          .addSink(new ClickHouseSink(props))
          .name("your_table ClickHouse sink);
```

## Roadmap
- [ ] reading files from "failed-records-path"
- [ ] migrate to gradle
