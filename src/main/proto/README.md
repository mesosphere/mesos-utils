# Google Protocol Buffers

Mesos-utils is using Google [Protocol Buffers](https://developers.google.com/protocol-buffers) to marshal and unmarshal data from the persistent store.

## Dependencies

- Google [Protocol Buffers](https://developers.google.com/protocol-buffers) v2.5 should be installed on the system.
-- Note: protoc 2.6 (and higher) will generate incompatible code. Make sure your version of protoc is 2.5.

## Rebuilt the protos

To rebuild the protos, open a terminal where the current working directory is the project directory.

```
$> cd src/main/proto
$> protoc --java_out=../java/ mesos-utils.proto
```

