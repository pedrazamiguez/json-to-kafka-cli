package es.pedrazamiguez;

import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "consume", description = "Consume Protobuf messages from Kafka and print them")
public class ConsumeCommand implements Callable<Integer> {

  @Option(names = "--topic", required = true, description = "Kafka topic to consume from")
  private String topic;

  @Option(names = "--bootstrap", defaultValue = "localhost:9092", description = "Kafka bootstrap servers")
  private String bootstrapServers;

  @Override
  public Integer call() throws Exception {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "cli-consumer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singleton(topic));
      System.out.println("Listening to topic: " + topic);

      while (true) {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, byte[]> record : records) {
          var msg = PersonOuter.Person.parseFrom(record.value());
          String json = JsonFormat.printer().print(msg);
          System.out.println(json);
        }
      }
    }
  }
}
