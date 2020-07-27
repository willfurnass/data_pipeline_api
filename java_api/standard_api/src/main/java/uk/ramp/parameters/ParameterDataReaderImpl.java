package uk.ramp.parameters;

import static com.google.common.base.Charsets.UTF_8;
import static java.nio.channels.Channels.newReader;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.ramp.file.CleanableFileChannel;
import uk.ramp.toml.TomlReader;

public class ParameterDataReaderImpl implements ParameterDataReader {
  private final TomlReader tomlReader;

  public ParameterDataReaderImpl(TomlReader tomlReader) {
    this.tomlReader = tomlReader;
  }

  @Override
  public ReadComponent read(CleanableFileChannel fileChannel, String component) {
    return tomlReader
        .read(newReader(fileChannel, UTF_8), new TypeReference<Components>() {})
        .components()
        .get(component);
  }
}
