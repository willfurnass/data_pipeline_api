package uk.ramp.toml;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;

public class TomlWriter {
  private final TOMLMapper tomlMapper;

  public TomlWriter(TOMLMapper tomlMapper) {
    this.tomlMapper = tomlMapper;
  }

  public <T> void write(Writer writer, T object) {
    try {
      tomlMapper.writeValue(writer, object);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
