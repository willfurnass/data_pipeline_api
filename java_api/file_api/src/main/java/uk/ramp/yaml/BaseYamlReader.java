package uk.ramp.yaml;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;

class BaseYamlReader implements YamlReader {

  @Override
  public <T> T read(Reader reader, TypeReference<T> typeReference) {
    try {
      return new YAMLMapper().registerModule(new Jdk8Module()).readValue(reader, typeReference);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
