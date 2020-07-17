package uk.ramp.access;

import java.io.Writer;
import uk.ramp.yaml.YamlWriter;

class AccessLogWriter {
  private final YamlWriter yamlWriter;
  private final Writer writer;

  AccessLogWriter(YamlWriter yamlWriter, Writer writer) {
    this.writer = writer;
    this.yamlWriter = yamlWriter;
  }

  public void write(ImmutableAccessLog data) {
    yamlWriter.write(writer, data);
  }
}
