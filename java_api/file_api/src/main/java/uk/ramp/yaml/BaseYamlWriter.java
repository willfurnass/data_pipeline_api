package uk.ramp.yaml;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;

public class BaseYamlWriter implements YamlWriter {
  private final ObjectMapper yamlMapper;

  BaseYamlWriter() {
    this.yamlMapper =
        new YAMLMapper()
            .disable(Feature.WRITE_DOC_START_MARKER)
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setSerializationInclusion(Include.NON_DEFAULT);
  }

  @Override
  public <T> void write(Writer writer, T data) {
    try {
      yamlMapper.writeValue(writer, data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
