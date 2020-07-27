package uk.ramp.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.guava.api.Assertions.assertThat;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.ramp.distribution.Distribution;
import uk.ramp.distribution.Distribution.DistributionType;
import uk.ramp.distribution.ImmutableDistribution;
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

  private final Distribution distribution =
      ImmutableDistribution.builder()
          .internalShape(1)
          .internalScale(2)
          .internalType(DistributionType.gamma)
          .build();
  private final Number estimate = 1.0;

  private String configPath;
  private String dataDirectoryPath;

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
    Files.deleteIfExists(Path.of(dataDirectoryPath, "parameter/runId.toml"));
    Files.deleteIfExists(Path.of("access-runId.yaml"));
    samples = ImmutableSamples.builder().addSamples(1, 2, 3).build();
  }

  @Test
  public void testReadEstimate() {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "parameter";
    String component = "example-estimate";
    assertThat(stdApi.readEstimate(dataProduct, component)).isEqualTo(estimate);
  }

  @Test
  public void testWriteEstimate() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "parameter";
    String component = "example-estimate-w";
    stdApi.writeEstimate(dataProduct, component, estimate);

    assertEqualFileContents("actualEstimate.toml", "expectedEstimate.toml");
  }

  @Test
  public void testReadDistribution() {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "parameter";
    String component = "example-distribution";

    assertThat(stdApi.readDistribution(dataProduct, component)).isEqualTo(distribution);
  }

  @Test
  public void testWriteDistribution() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "parameter";
    String component = "example-distribution-w";
    stdApi.writeDistribution(dataProduct, component, distribution);

    assertEqualFileContents("actualDistribution.toml", "expectedDistribution.toml");
  }

  @Test
  public void testReadSample() {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "parameter";
    String component = "example-samples";
    assertThat(stdApi.readSamples(dataProduct, component)).containsExactly(1, 2, 3);
  }

  @Test
  public void testWriteSamples() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "parameter";
    String component = "example-samples-w";
    stdApi.writeSamples(dataProduct, component, samples);

    assertEqualFileContents("actualSamples.toml", "expectedSamples.toml");
  }

  @Test
  public void testWriteSamplesMultipleComponents() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath));
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
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "object";
    String component = "grid1km/10year/females";

    assertThat(stdApi.readArray(dataProduct, component)).isEqualTo(array);
  }

  @Test
  @Ignore // Not implemented yet
  public void testWriteArray() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "object";
    String component = "example-array-w";
    stdApi.writeArray(dataProduct, component, array);

    assertEqualFileContents("actualArray.h5", "expectedArray.h5");
  }

  @Test
  @Ignore // Not implemented yet
  public void testReadTable() {
    var stdApi = new StandardApi(Path.of(configPath));
    String dataProduct = "object";
    String component = "example-table";

    assertThat(stdApi.readTable(dataProduct, component)).isEqualTo(mockTable);
  }

  @Test
  @Ignore // Not implemented yet
  public void testWriteTable() throws IOException {
    var stdApi = new StandardApi(Path.of(configPath));
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
