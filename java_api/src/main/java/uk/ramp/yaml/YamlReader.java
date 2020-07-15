package uk.ramp.yaml;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.Reader;

public interface YamlReader {
  <T> T read(Reader reader, TypeReference<T> typeReference);
}
