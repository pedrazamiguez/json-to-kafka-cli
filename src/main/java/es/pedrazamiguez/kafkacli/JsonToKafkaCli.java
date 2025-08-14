package es.pedrazamiguez.kafkacli;

import es.pedrazamiguez.kafkacli.command.ConsumeCommand;
import es.pedrazamiguez.kafkacli.command.SendCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine;


@Command(
    name = "json2kafka",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "CLI tool to send JSON documents as Protobuf messages to Kafka broker",
    subcommands = {
        SendCommand.class,
        ConsumeCommand.class
    }
)
public class JsonToKafkaCli {

  public static void main(String[] args) {
    final int exitCode = new CommandLine(new JsonToKafkaCli()).execute(args);
    System.exit(exitCode);
  }
}
