package uk.ramp.parameters;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.file.CleanableFileChannel;
import uk.ramp.toml.TomlWriter;

public class ParameterDataWriterImplTest {
  private CleanableFileChannel fileChannel;
  private TomlWriter tomlWriter;

  @Before
  public void setUp() throws Exception {
    fileChannel = mock(CleanableFileChannel.class);
    tomlWriter = mock(TomlWriter.class);
    when(fileChannel.size()).thenReturn(64L);
  }

  @Test
  public void writeEstimate() throws IOException {
    var dataWriter = new ParameterDataWriterImpl(tomlWriter);
    var estimate = ImmutableEstimate.builder().internalValue(5).build();
    var components = ImmutableComponents.builder().putComponents("component", estimate).build();
    dataWriter.write(fileChannel, "component", estimate);

    verify(fileChannel).size();
    verify(fileChannel).position(64);
    verify(tomlWriter).write(any(), eq(components));
    verifyNoMoreInteractions(fileChannel);
  }
}
