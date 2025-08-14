package es.pedrazamiguez.kafkacli.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.pedrazamiguez.PersonOuter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "send", description = "Send a JSON file as a Protobuf message to Kafka")
public class SendCommand implements Callable<Integer> {

  @Parameters(index = "0", description = "Path to the JSON file")
  private File jsonFile;

  @Option(names = "--topic", required = true, description = "Kafka topic to send to")
  private String topic;

  @Option(names = "--bootstrap", defaultValue = "localhost:9092", description = "Kafka bootstrap servers")
  private String bootstrapServers;

  @Override
  public Integer call() throws Exception {
    byte[] jsonBytes = Files.readAllBytes(jsonFile.toPath());

    // JSON → Protobuf mapping
    ObjectMapper mapper = new ObjectMapper();
    var jsonNode = mapper.readTree(jsonBytes);

    PersonOuter.Person.Builder builder =
        PersonOuter.Person.newBuilder();

    builder.setName(jsonNode.get("name").asText());
    builder.setId(jsonNode.get("id").asInt());
    if (jsonNode.has("email")) {
      builder.setEmail(jsonNode.get("email").asText());
    }

    var message = builder.build();

    // Kafka producer setup
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, message.toByteArray())).get();
      System.out.println("Message sent to topic: " + topic);
    }

    return 0;
  }
}
