package uk.ramp.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.guava.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.ramp.distribution.Distribution;
import uk.ramp.distribution.Distribution.DistributionType;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.distribution.ImmutableMinMax;
import uk.ramp.distribution.MinMax;
import uk.ramp.samples.ImmutableSamples;
import uk.ramp.samples.Samples;

public class StandardApiIntegrationTest {
  private final Table<Integer, String, Number> mockTable =
      ImmutableTable.<Integer, String, Number>builder()
          .put(0, "colA", 5)
          .put(1, "colA", 6)
          .put(2, "colA", 7)
          .put(0, "colB", 0)
          .put(1, "colB", 1)
          .put(2, "colB", 2)
          .build();

  private final Number[] array = new Number[] {5, 6, 3.4};
  private Samples samples;

  private Distribution distribution;
  private Distribution categoricalDistribution;
  private Number estimate = 1.0;
  private String configPath;
  private String dataDirectoryPath;
  private RandomGenerator rng;

  @Before
  public void setUp() throws Exception {
    configPath = Paths.get(getClass().getResource("/config.yaml").toURI()).toString();
    String parentPath = Path.of(configPath).getParent().toString();
    dataDirectoryPath = Path.of(parentPath, "folder/data").toString();
    Files.deleteIfExists(Path.of(dataDirectoryPath, "exampleWrite.toml"));
    Files.deleteIfExists(Path.of(dataDirectoryPath, "actualEstimate.toml"));
    Files.deleteIfExists(Path.of(dataDirectoryPath, "actualSamples.toml"));
    Files.deleteIfExists(Path.of(dataDirectoryPath, "actualSamplesMultiple.toml"));
    Files.deleteIfExists(Path.of(dataDirectoryPath, "actualDistribution.toml"));
    Files.deleteIfExists(Path.of(dataDirectoryPath, "actualDistributionCategorical.toml"));
    Files.deleteIfExists(Path.of(dataDirectoryPath, "parameter/runId.toml"));
    Files.deleteIfExists(Path.of("access-runId.yaml"));
    rng = mock(RandomGenerator.class);
    when(rng.nextDouble()).thenReturn(0D);
    samples = ImmutableSamples.builder().addSamples(1, 2, 3).rng(rng).build();

    distribution =
        ImmutableDistribution.builder()
            .internalShape(1)
            .internalScale(2)
            .internalType(DistributionType.gamma)
            .rng(rng)
            .build();

    MinMax firstMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(0)
            .upperBoundary(4)
            .build();

    MinMax secondMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(5)
            .upperBoundary(9)
            .build();

    MinMax thirdMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(10)
            .upperBoundary(14)
            .build();

    MinMax fourthMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(15)
            .upperBoundary(20)
            .build();

    categoricalDistribution =
        ImmutableDistribution.builder()
            .internalType(DistributionType.categorical)
            .bins(List.of(firstMinMax, secondMinMax, thirdMinMax, fourthMinMax))
            .weights(List.of(0.4, 0.1, 0.1, 0.4))
            .rng(rng)
            .build();
  }

  @Test
  public void testReadEstimate() {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-estimate";
    assertThat(stdApi.readEstimate(dataProduct, component)).isEqualTo(estimate);
  }

  @Test
  public void testWriteEstimate() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-estimate-w";
    stdApi.writeEstimate(dataProduct, component, estimate);

    assertEqualFileContents("actualEstimate.toml", "expectedEstimate.toml");
  }

  @Test
  public void testReadDistribution() {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-distribution";

    assertThat(stdApi.readDistribution(dataProduct, component)).isEqualTo(distribution);
  }

  @Test
  public void testReadCategoricalDistribution() {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-distribution-categorical";

    assertThat(stdApi.readDistribution(dataProduct, component)).isEqualTo(categoricalDistribution);
  }

  @Test
  public void testWriteDistribution() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-distribution-w";
    stdApi.writeDistribution(dataProduct, component, distribution);

    assertEqualFileContents("actualDistribution.toml", "expectedDistribution.toml");
  }

  @Test
  public void testWriteCategoricalDistribution() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-distribution-w-categorical";
    stdApi.writeDistribution(dataProduct, component, categoricalDistribution);

    assertEqualFileContents(
        "actualDistributionCategorical.toml", "expectedDistributionCategorical.toml");
  }

  @Test
  public void testReadSample() {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-samples";
    assertThat(stdApi.readSamples(dataProduct, component)).containsExactly(1, 2, 3);
  }

  @Test
  public void testWriteSamples() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component = "example-samples-w";
    stdApi.writeSamples(dataProduct, component, samples);

    assertEqualFileContents("actualSamples.toml", "expectedSamples.toml");
  }

  @Test
  public void testWriteSamplesMultipleComponents() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "parameter";
    String component1 = "example-samples-w1";
    String component2 = "example-samples-w2";
    stdApi.writeSamples(dataProduct, component1, samples);
    stdApi.writeSamples(dataProduct, component2, samples);

    assertEqualFileContents("actualSamplesMultiple.toml", "expectedSamplesMultiple.toml");
  }

  @Test
  @Ignore // Not implemented yet
  public void testReadArray() {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "object";
    String component = "grid1km/10year/females";

    assertThat(stdApi.readArray(dataProduct, component)).isEqualTo(array);
  }

  @Test
  @Ignore // Not implemented yet
  public void testWriteArray() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "object";
    String component = "example-array-w";
    stdApi.writeArray(dataProduct, component, array);

    assertEqualFileContents("actualArray.h5", "expectedArray.h5");
  }

  @Test
  @Ignore // Not implemented yet
  public void testReadTable() {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "object";
    String component = "example-table";

    assertThat(stdApi.readTable(dataProduct, component)).isEqualTo(mockTable);
  }

  @Test
  @Ignore // Not implemented yet
  public void testWriteTable() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath), rng);
    String dataProduct = "object";
    String component = "example-table-w";

    stdApi.writeTable(dataProduct, component, mockTable);

    assertEqualFileContents("actualTable.h5", "expectedTable.h5");
  }

  private void assertEqualFileContents(String file1, String file2) throws IOException {
    assertThat(Files.readString(Path.of(dataDirectoryPath, file1)))
        .isEqualTo(Files.readString(Path.of(dataDirectoryPath, file2)));
  }
}
