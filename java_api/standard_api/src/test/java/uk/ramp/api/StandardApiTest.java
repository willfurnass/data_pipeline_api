package uk.ramp.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.ramp.distribution.Distribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.file.CleanableFileChannel;
import uk.ramp.parameters.Component;
import uk.ramp.parameters.ParameterDataReader;
import uk.ramp.parameters.ParameterDataWriter;
import uk.ramp.samples.Samples;

public class StandardApiTest {
  private FileApi fileApi;
  private ParameterDataReader parameterDataReader;
  private ParameterDataWriter parameterDataWriter;
  private CleanableFileChannel fileChannel;
  private Distribution distribution;
  private Samples samples;
  private Component component;
  private RandomGenerator rng;

  @Before
  public void setUp() throws Exception {
    this.fileApi = mock(FileApi.class);
    this.parameterDataReader = mock(ParameterDataReader.class);
    this.parameterDataWriter = mock(ParameterDataWriter.class);
    this.fileChannel = mock(CleanableFileChannel.class);
    this.distribution = mock(Distribution.class);
    this.samples = mock(Samples.class);
    this.component = mock(Component.class);
    this.rng = mock(RandomGenerator.class);

    when(fileApi.openForRead(any())).thenReturn(fileChannel);
    when(fileApi.openForWrite(any())).thenReturn(fileChannel);
    when(parameterDataReader.read(fileChannel, "component")).thenReturn(component);
    when(rng.nextDouble()).thenReturn(0D);
  }

  @Test
  public void readEstimate() {
    when(component.getEstimate()).thenReturn(5);
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    assertThat(api.readEstimate("dataProduct", "component")).isEqualTo(5);
  }

  @Test
  public void writeEstimate() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    api.writeEstimate("dataProduct", "component", 5);

    var expectedParameterData = ImmutableEstimate.builder().internalValue(5).rng(rng).build();
    verify(parameterDataWriter).write(fileChannel, "component", expectedParameterData);
  }

  @Test
  public void readDistribution() {
    when(component.getDistribution()).thenReturn(distribution);
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    assertThat(api.readDistribution("dataProduct", "component")).isEqualTo(distribution);
  }

  @Test
  public void writeDistribution() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    api.writeDistribution("dataProduct", "component", distribution);
    verify(parameterDataWriter).write(fileChannel, "component", distribution);
  }

  @Test
  public void readSamples() {
    when(component.getSamples()).thenReturn(List.of(1, 2, 3));
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    assertThat(api.readSamples("dataProduct", "component")).containsExactly(1, 2, 3);
  }

  @Test
  public void writeSamples() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    api.writeSamples("dataProduct", "component", samples);
    verify(parameterDataWriter).write(fileChannel, "component", samples);
  }

  @Test
  @Ignore // TODO additional functionality to implement for future improvement
  public void writeMultipleEstimatesSameKey() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    api.writeEstimate("dataProduct", "component", 1);
    assertThatIllegalStateException()
        .isThrownBy(() -> api.writeEstimate("dataProduct", "component", 2));
  }

  @Test
  @Ignore // TODO additional functionality to implement for future improvement
  public void writeMultipleEstimatesSameValue() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    api.writeEstimate("dataProduct", "component", 1);
    assertThatIllegalStateException()
        .isThrownBy(() -> api.writeEstimate("dataProduct", "component", 1));
  }

  @Test
  public void writeMultipleEstimatesDifferentKey() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    var expectedParameterData1 = ImmutableEstimate.builder().internalValue(1).rng(rng).build();
    var expectedParameterData2 = ImmutableEstimate.builder().internalValue(2).rng(rng).build();
    api.writeEstimate("dataProduct", "component1", 1);
    api.writeEstimate("dataProduct", "component2", 2);
    verify(parameterDataWriter).write(fileChannel, "component1", expectedParameterData1);
    verify(parameterDataWriter).write(fileChannel, "component2", expectedParameterData2);
  }

  @Test
  @Ignore // Not implemented yet
  public void readArray() {
    var api = new StandardApi(fileApi, parameterDataReader, parameterDataWriter, rng);
    assertThat(api.readArray("dataProduct", "component").as1DArray()).containsExactly(1, 2, 3);
  }

  @Test
  @Ignore // Not implemented yet
  public void writeTable() {
    assertThat(true).isFalse();
  }

  @Test
  @Ignore // Not implemented yet
  public void readTable() {
    assertThat(true).isFalse();
  }

  @Test
  @Ignore // Not implemented yet
  public void writeArray() {
    assertThat(true).isFalse();
  }
}
