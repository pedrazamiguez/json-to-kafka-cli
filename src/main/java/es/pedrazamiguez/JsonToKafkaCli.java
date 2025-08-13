package es.pedrazamiguez;

import picocli.CommandLine.Command;
import picocli.CommandLine;


@Command(
    name = "json2kafka",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "CLI tool for sending JSON as Protobuf to Kafka broker",
    subcommands = {
        SendCommand.class,
        ConsumeCommand.class
    }
)
public class JsonToKafkaCli {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new JsonToKafkaCli()).execute(args);
    System.exit(exitCode);
  }
}
