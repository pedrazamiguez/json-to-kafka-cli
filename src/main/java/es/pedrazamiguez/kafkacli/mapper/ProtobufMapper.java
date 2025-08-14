package es.pedrazamiguez.kafkacli.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import es.pedrazamiguez.kafkacli.PersonOuter;

import java.io.IOException;

public final class ProtobufMapper {

  private ProtobufMapper() {
  }

  public static PersonOuter.Person toPerson(final byte[] jsonBytes) throws IOException {
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

}
