# Flink EMQX Connector

## Quick start

See [`WordCount.java`](./flink-emqx-connector-examples/flink-emqx-connector-examples-word-count/src/main/java/com/emqx/flink/connector/examples/wordcount/WordCount.java) for a simple example to get started.

## Developing

### Building

```sh
mvn clean package
# without running tests
mvn clean package -DskipTests
```

### Testing

```sh
mvn clean test
# Run specific test suite
mvn clean -Dtest=EMQXSourceIntegrationTests test
# Run specific test case
mvn clean -Dtest=EMQXSourceIntegrationTests#recoverAfterFailure test
```
