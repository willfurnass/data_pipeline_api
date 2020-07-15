package uk.ramp.yaml;

import java.io.Writer;

public interface YamlWriter {
  <T> void write(Writer writer, T data);
}
