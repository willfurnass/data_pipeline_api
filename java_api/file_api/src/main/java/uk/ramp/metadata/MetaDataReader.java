package uk.ramp.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import uk.ramp.yaml.YamlReader;

class MetaDataReader {
  private static final String LOCATION = "metadata.yaml";
  private final YamlReader yamlReader;
  private final Path absoluteLocationPath;

  MetaDataReader(YamlReader yamlReader, String dataDirectory) {
    this.yamlReader = yamlReader;
    this.absoluteLocationPath = Path.of(dataDirectory, LOCATION);
  }

  public List<ImmutableMetadataItem> read() {
    try {
      return yamlReader.read(
          Files.newBufferedReader(absoluteLocationPath), new TypeReference<>() {});
    } catch (IOException e) {
      throw new UncheckedIOException(new IOException(e));
    }
  }
}
