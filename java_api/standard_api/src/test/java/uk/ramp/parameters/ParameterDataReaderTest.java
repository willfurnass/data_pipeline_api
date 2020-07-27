package uk.ramp.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Reader;
import org.junit.Before;
import org.junit.Test;
import uk.ramp.distribution.Distribution;
import uk.ramp.estimate.Estimate;
import uk.ramp.file.CleanableFileChannel;
import uk.ramp.samples.Samples;
import uk.ramp.toml.TomlReader;

public class ParameterDataReaderTest {
  private CleanableFileChannel fileChannel;
  private TomlReader tomlReader;
  private Estimate mockEstimate;

  @Before
  public void setUp() throws Exception {
    this.fileChannel = mock(CleanableFileChannel.class);
    this.tomlReader = mock(TomlReader.class);
    this.mockEstimate = mock(Estimate.class);
  }

  @Test
  public void read() {
    Components expectedComponents =
        ImmutableComponents.builder()
            .putComponents("example-estimate", mockEstimate)
            .putComponents("example-distribution", mock(Distribution.class))
            .putComponents("example-samples", mock(Samples.class))
            .build();

    when(tomlReader.read(any(Reader.class), any())).thenReturn(expectedComponents);
    var dataReader = new ParameterDataReaderImpl(tomlReader);

    assertThat(dataReader.read(fileChannel, "example-estimate")).isEqualTo(mockEstimate);
  }
}
