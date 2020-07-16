package uk.ramp.access;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import uk.ramp.config.Config;
import uk.ramp.hash.Hasher;
import uk.ramp.yaml.YamlWriter;

public class AccessLoggerFactory {
  public AccessLogger accessLogger(
      Config config, YamlWriter yamlWriter, Clock clock, Instant openTimestamp, Hasher hasher) {
    if (config.accessLogDisabled()) {
      return new NoImplAccessLogger();
    }
    var accessLogPath = config.normalisedAccessLogPath();

    BufferedWriter underlyingWriter;
    try {
      underlyingWriter = Files.newBufferedWriter(Path.of(accessLogPath));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    AccessLogWriter writer = new AccessLogWriter(yamlWriter, underlyingWriter);
    return new AccessLoggerImpl(new ArrayList<>(), clock, writer, config, openTimestamp, hasher);
  }
}
