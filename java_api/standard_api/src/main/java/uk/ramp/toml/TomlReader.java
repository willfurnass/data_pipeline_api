package uk.ramp.toml;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;

public class TomlReader {
  private final TOMLMapper tomlMapper;

  public TomlReader(TOMLMapper tomlMapper) {
    this.tomlMapper = tomlMapper;
  }

  public <T> T read(Reader reader, TypeReference<T> typeReference) {
    try {
      return tomlMapper.readValue(reader, typeReference);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
