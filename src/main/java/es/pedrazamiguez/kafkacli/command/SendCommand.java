package es.pedrazamiguez.kafkacli.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.pedrazamiguez.PersonOuter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "send", description = "Send a JSON file as a Protobuf message to Kafka")
public class SendCommand implements Callable<Integer> {

  private static final Logger log = LoggerFactory.getLogger(SendCommand.class);

  @Parameters(
      index = "0",
      description = "Path to the file containing the JSON document"
  )
  private File jsonFile;

  @Option(
      names = {"--topic", "-t"},
      required = true,
      description = "Kafka topic to send messages to"
  )
  private String topic;

  @Option(
      names = {"--bootstrap", "-b"},
      defaultValue = "localhost:9092",
      description = "Comma-separated list of Kafka bootstrap servers in the " + "format host1:port1,host2:port2,..."
  )
  private String bootstrapServers;

  @Override
  public Integer call() throws Exception {
    final byte[] jsonBytes = this.readJsonFile(jsonFile);
    final PersonOuter.Person message = this.toPerson(jsonBytes);

    final Properties producerProperties = this.createProducerProperties();

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties)) {
      producer.send(new ProducerRecord<>(topic, message.toByteArray())).get();
      log.info("Message sent to topic: {}", topic);
    }

    return 0;
  }

  private byte[] readJsonFile(final File jsonFile) throws Exception {
    return Files.readAllBytes(jsonFile.toPath());
  }

  private PersonOuter.Person toPerson(final byte[] jsonBytes) throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonNode jsonNode = mapper.readTree(jsonBytes);

    final PersonOuter.Person.Builder builder = PersonOuter.Person.newBuilder();

    builder.setName(jsonNode.get("name").asText());
    builder.setId(jsonNode.get("id").asInt());
    if (jsonNode.has("email")) {
      builder.setEmail(jsonNode.get("email").asText());
    }

    return builder.build();
  }

  private Properties createProducerProperties() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return props;
  }
}
