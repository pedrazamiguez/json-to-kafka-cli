package es.pedrazamiguez.kafkacli.command;

import com.google.protobuf.util.JsonFormat;
import es.pedrazamiguez.kafkacli.PersonOuter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "consume", description = "Consume Protobuf messages from Kafka and print them as JSON")
public class ConsumeCommand implements Callable<Integer> {

  private static final Logger log = LoggerFactory.getLogger(ConsumeCommand.class);

  @Option(
      names = {"--topic", "-t"},
      required = true,
      description = "Kafka topic to consume messages from"
  )
  private String topic;

  @Option(
      names = {"--group", "-g"},
      defaultValue = "cli-consumer",
      description = "Kafka consumer group ID"
  )
  private String groupId;

  @Option(
      names = {"--offset-reset", "-o"},
      defaultValue = "earliest",
      description = "Offset reset policy (earliest/latest/none)"
  )
  private String offsetReset;

  @Option(
      names = {"--bootstrap", "-b"},
      defaultValue = "localhost:9092",
      description = "Comma-separated list of Kafka bootstrap servers in the format host1:port1,host2:port2,..."
  )
  private String bootstrapServers;

  @Override
  public Integer call() throws Exception {
    final Properties consumerProperties = this.createConsumerProperties();

    try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
      consumer.subscribe(Collections.singleton(topic));
      log.info("Listening to topic: {}", topic);

      while (true) {
        final ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofSeconds(1));
        for (final ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
          final var protoMessage = PersonOuter.Person.parseFrom(consumerRecord.value());
          final String jsonMessage = JsonFormat.printer().print(protoMessage);
          log.info("""
              Message consumed:
              
              {}
              
              """, jsonMessage);
        }
      }
    }
  }

  private Properties createConsumerProperties() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.offsetReset);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }
}
