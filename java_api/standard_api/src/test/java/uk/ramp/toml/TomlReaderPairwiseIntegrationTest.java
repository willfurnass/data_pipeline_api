package uk.ramp.toml;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.StringReader;
import org.junit.Test;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.parameters.Components;
import uk.ramp.parameters.ImmutableComponents;

public class TomlReaderPairwiseIntegrationTest {
  private final String toml =
      "[example-estimate]\n" + "type = \"point-estimate\"\n" + "value = 1.0";

  @Test
  public void read() throws IOException {
    TomlReader tomlReader = new TomlReader(new TOMLMapper());
    var reader = new StringReader(toml);
    var estimate = ImmutableEstimate.builder().internalValue(1.0).build();
    Components components =
        ImmutableComponents.builder().putComponents("example-estimate", estimate).build();

    assertThat(tomlReader.read(reader, new TypeReference<Components>() {})).isEqualTo(components);
  }
}
