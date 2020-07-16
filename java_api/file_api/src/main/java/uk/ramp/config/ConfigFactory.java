package uk.ramp.config;

import java.nio.file.Path;
import java.time.Instant;
import uk.ramp.hash.Hasher;
import uk.ramp.yaml.YamlReader;

public class ConfigFactory {
  public Config config(
      YamlReader yamlReader, Hasher hasher, Instant openTimestamp, Path configFilePath) {
    var config = new ConfigReader(yamlReader, configFilePath).read();
    var freshHash = hasher.fileHash(configFilePath.toString(), openTimestamp);
    var runId = config.runId().orElse(freshHash);
    return config.withRunId(runId).withParentPath(configFilePath.getParent().toString());
  }
}
