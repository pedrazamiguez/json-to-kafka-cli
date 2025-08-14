# JSON to Kafka CLI Tool

This is a CLI tool to send JSON documents as Protobuf messages to a Kafka broker.

## Features

- Convert JSON documents to Protobuf messages and send them to Kafka.
- Consume Protobuf messages from Kafka and display them as JSON.

## Running Kafka on Docker

For convenience, there is a `docker-compose.yml` file that spins up a Kafka broker and a Zookeeper instance, as well as RedPanda, a web-based Kafka UI tool.

The mentioned `docker-compose.yml` file references environment variables that are expected to be read from a `.env` file.
You can rename or copy the `.env.example` file to `.env` and adjust the values as needed, although you should not need to.

```bash
cp .env.example .env
```

```bash
docker compose up -d
```

## How to prepare the CLI tool

Use the gradle wrapper to create a jar file as follows:

```bash
./gradlew fatJar
```

## How to run the CLI tool

Once the jar has been generated, you can execute it as follows:

```bash
java -jar build/libs/json-to-kafka-cli-1.0-SNAPSHOT-all.jar --help
```

This command will show the available sub-commands and options.

## Sending a message

## Consuming messages

## Displaying Kafka topics and consumer groups on RedPanda

## References

- [Protobuf Gradle Plugin](https://github.com/google/protobuf-gradle-plugin)

