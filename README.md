# Dynamic Protobuf-3.x format support for Apache Flink

This project is an adapter to connect [Google Protobuf](https://developers.google.com/protocol-buffers) to the flink's
own `TypeInformation`-based [serialization framework](https://flink.apache.org/news/2020/04/15/flink-serialization-tuning-vol-1.html).

TestCode is copy from official [flink-protobuf](https://github.com/apache/flink/tree/master/flink-formats/flink-protobuf), and modified to support dynamic protobuf test.

How to create a table with DynamicProtobuf format
----------------

Here is an example to create a table using the Kafka connector and DynamicProtobuf format.

Below is the proto definition file.

```
syntax = "proto3";
package com.example;
option java_package = "com.example";
option java_multiple_files = true;

message SimpleTest {
    optional int64 uid = 1;
    optional string name = 2;
    optional int32 category_type = 3;
    optional bytes content = 4;
    optional double price = 5;
    map<int64, InnerMessageTest> value_map = 6;
    repeated  InnerMessageTest value_arr = 7;
    
    message InnerMessageTest{
      optional int64 v1 =1;
      optional int32 v2 =2;
    }
}
```

Define your table structure. This implementation does not require adding the compiled protobuf classes to the classpath; it directly parses the protobuf `byte[]` based on the field structure of the table. However, there are a few requirements:

1. Fields must start from 1, and there can be no missing indices in between.
2. Since there is no original ProtoBuf definition, it cannot include Enum types.

```sql
CREATE TABLE simple_test (
  uid BIGINT,
  name STRING,
  category_type INT,
  content BINARY,
  price DOUBLE,
  value_map map<BIGINT, row<v1 BIGINT, v2 INT>>,
  value_arr array<row<v1 BIGINT, v2 INT>>
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'dynamic-protobuf3'
)
```

Format Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 8%">Forwarded</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what format to use, here should be <code>'dynamic-protobuf3'</code>.</td>
    </tr>
    <tr>
      <td><h5>protobuf.read-default-values</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
          If this value is set to true, primtive types will be set to default values 
          instead of null
      </td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

The following table lists the type mapping from Flink type to Protobuf type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Protobuf type</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>bool</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>bytes</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>int32</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>int64</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>float</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>repeated</code></td>
      <td>Elements cannot be null, the string default value can be specified by <code>write-null-string-literal</code></td>
    </tr>
    <tr>
      <td><code>MAP</code></td>
      <td><code>map</code></td>
      <td>Keys or values cannot be null, the string default value can be specified by <code>write-null-string-literal</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>message</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>ROW&lt;seconds BIGINT, nanos INT&gt;</code></td>
      <td><code>google.protobuf.timestamp</code></td>
      <td>The google.protobuf.timestamp type can be mapped to seconds and fractions of seconds at nanosecond resolution in UTC epoch time using the row type as well as the protobuf definition.</td>
    </tr>
    </tbody>
</table>

Null Values
----------------
As protobuf does not permit null values in maps and array, we need to auto-generate default values when converting from Flink Rows to Protobuf.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Protobuf Data Type</th>
        <th class="text-left">Default Value</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>int32 / int64 / float / double</td>
      <td>0</td>
    </tr>
    <tr>
      <td>string</td>
      <td>""</td>
    </tr>
    <tr>
      <td>bool</td>
      <td>false</td>
    </tr>
    <tr>
      <td>binary</td>
      <td>ByteString.EMPTY</td>
    </tr>
    <tr>
      <td>message</td>
      <td>MESSAGE.getDefaultInstance()</td>
    </tr>
    </tbody>
</table>

OneOf field
----------------
In the serialization process, there's no guarantee that the Flink fields of the same one-of group only contain at most one valid value.
When serializing, each field is set in the order of Flink schema, so the field in the higher position will override the field in lower position in the same one-of group.

You can refer to [Language Guide (proto3)](https://developers.google.com/protocol-buffers/docs/proto3) for more information about Protobuf types.
