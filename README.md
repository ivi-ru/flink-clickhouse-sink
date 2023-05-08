
# Flink-ClickHouse-Sink

![Java CI](https://github.com/ivi-ru/flink-clickhouse-sink/actions/workflows/maven.yml/badge.svg)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/ru.ivi.opensource/flink-clickhouse-sink/badge.svg)](https://maven-badges.herokuapp.com/maven-central/ru.ivi.opensource/flink-clickhouse-sink/)

## Description

[Flink](https://github.com/apache/flink) sink for [ClickHouse](https://github.com/yandex/ClickHouse) database. 
Powered by [Async Http Client](https://github.com/AsyncHttpClient/async-http-client).

High-performance library for loading data to ClickHouse. 

It has two triggers for loading data:
_by timeout_ and _by buffer size_.

##### Version map
|flink    | flink-clickhouse-sink | 
|:-------:|:---------------------:| 
|1.3.*    |         1.0.0         |
|1.9.*    |         1.3.4         |
|1.9.*    |         1.4.*         |

### Install

##### Maven Central

```xml
<dependency>
  <groupId>ru.ivi.opensource</groupId>
  <artifactId>flink-clickhouse-sink</artifactId>
  <version>1.4.0</version>
</dependency>
```

## Usage
### Properties
The flink-clickhouse-sink uses two parts of configuration properties: 
common and for each sink in you operators chain.

**The common part** (use like global):

 `clickhouse.sink.num-writers` - number of writers, which build and send requests, 
 
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

#### Configuration: global parameters

At first, you add global parameters for the Flink environment:
```java
StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
Map<String, String> globalParameters = new HashMap<>();

// ClickHouse cluster properties
globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, ...);
globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, ...);
globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, ...);

// sink common
globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, ...);
globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, ...);
globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, ...);
globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, ...);
globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, ...);
globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, ...);

// set global paramaters
ParameterTool parameters = ParameterTool.fromMap(buildGlobalParameters(config));
environment.getConfig().setGlobalJobParameters(parameters);

```

#### Converter

The main thing: the clickhouse-sink works with events in _String_
(ClickHouse insert format, like CSV) format.
You have to convert your event to csv format (like usual insert into a database).

For example, you have an event-pojo:
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
You have to implement a converter to csv, using
```java

public interface ClickHouseSinkConverter<T> {
 ...
}
```

You convert the pojo like this:

```java
import ru.ivi.opensource.flinkclickhousesink.ClickHouseSinkConverter;

public class YourEventConverter implements ClickHouseSinkConverter<A>{
    
    @Override
    public String convert(A record){
     StringBuilder builder = new StringBuilder();
     builder.append("(");

     // add a.str
     builder.append("'");
     builder.append(a.str);
     builder.append("', ");

     // add a.integer
     builder.append(String.valueOf(a.integer));
     builder.append(" )");
     return builder.toString();
    }
}
```

And then add your sink to the chain:
```java

// create table props for sink
Properties props = new Properties();
props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "your_table");
props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10000");

// converter
YourEventConverter converter = new YourEventConverter();       

// build chain
DataStream<YourEvent> dataStream = ...;
dataStream.addSink(new ClickHouseSink(props, converter))
          .name("your_table ClickHouse sink);
```

## Roadmap
- [ ] reading files from "failed-records-path"
- [ ] migrate to gradle
