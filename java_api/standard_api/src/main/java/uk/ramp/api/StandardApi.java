package uk.ramp.api;

import com.google.common.collect.Table;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.math3.random.RandomGenerator;
import uk.ramp.distribution.Distribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.file.CleanableFileChannel;
import uk.ramp.metadata.ImmutableMetadataItem;
import uk.ramp.objects.NumericalArray;
import uk.ramp.parameters.ParameterDataReader;
import uk.ramp.parameters.ParameterDataReaderImpl;
import uk.ramp.parameters.ParameterDataWriter;
import uk.ramp.parameters.ParameterDataWriterImpl;
import uk.ramp.parameters.ReadComponent;
import uk.ramp.samples.Samples;
import uk.ramp.toml.TOMLMapper;
import uk.ramp.toml.TomlReader;
import uk.ramp.toml.TomlWriter;

public class StandardApi implements AutoCloseable {
  private static final Cleaner cleaner = Cleaner.create(); // safety net for closing
  private final Cleanable cleanable;
  private final CleanableFileApi fileApi;
  private final ParameterDataReader parameterDataReader;
  private final ParameterDataWriter parameterDataWriter;
  private final RandomGenerator rng;

  public StandardApi(Path configPath, RandomGenerator rng) {
    this(
        new FileApi(configPath),
        new ParameterDataReaderImpl(new TomlReader(new TOMLMapper(rng))),
        new ParameterDataWriterImpl(new TomlWriter(new TOMLMapper(rng))),
        rng);
  }

  StandardApi(
      FileApi fileApi,
      ParameterDataReader parameterDataReader,
      ParameterDataWriter parameterDataWriter,
      RandomGenerator rng) {
    this.fileApi = new CleanableFileApi(fileApi);
    this.parameterDataReader = parameterDataReader;
    this.parameterDataWriter = parameterDataWriter;
    this.rng = rng;
    this.cleanable = cleaner.register(this, this.fileApi);
  }

  private static class CleanableFileApi implements Runnable {
    private final FileApi fileApi;

    CleanableFileApi(FileApi fileApi) {
      this.fileApi = fileApi;
    }

    @Override
    public void run() {
      fileApi.close();
    }
  }

  public Number readEstimate(String dataProduct, String component) {
    var query =
        ImmutableMetadataItem.builder().dataProduct(dataProduct).component(component).build();

    ReadComponent data;
    try (CleanableFileChannel fileChannel = fileApi.fileApi.openForRead(query)) {
      data = parameterDataReader.read(fileChannel, component);
    }
    return data.getEstimate();
  }

  public void writeEstimate(String dataProduct, String component, Number estimateNumber) {
    var query =
        ImmutableMetadataItem.builder()
            .dataProduct(dataProduct)
            .component(component)
            .extension("toml")
            .build();
    var estimate = ImmutableEstimate.builder().internalValue(estimateNumber).rng(rng).build();

    try (CleanableFileChannel fileChannel = fileApi.fileApi.openForWrite(query)) {
      parameterDataWriter.write(fileChannel, component, estimate);
    }
  }

  public Distribution readDistribution(String dataProduct, String component) {
    var query =
        ImmutableMetadataItem.builder().dataProduct(dataProduct).component(component).build();

    ReadComponent data;
    try (CleanableFileChannel fileChannel = fileApi.fileApi.openForRead(query)) {
      data = parameterDataReader.read(fileChannel, component);
    }
    return data.getDistribution();
  }

  public void writeDistribution(String dataProduct, String component, Distribution distribution) {
    var query =
        ImmutableMetadataItem.builder()
            .dataProduct(dataProduct)
            .component(component)
            .extension("toml")
            .build();

    try (CleanableFileChannel fileChannel = fileApi.fileApi.openForWrite(query)) {
      parameterDataWriter.write(fileChannel, component, distribution);
    }
  }

  public List<Number> readSamples(String dataProduct, String component) {
    var query =
        ImmutableMetadataItem.builder().dataProduct(dataProduct).component(component).build();

    ReadComponent data;
    try (CleanableFileChannel fileChannel = fileApi.fileApi.openForRead(query)) {
      data = parameterDataReader.read(fileChannel, component);
    }
    return data.getSamples();
  }

  public void writeSamples(String dataProduct, String component, Samples samples) {
    var query =
        ImmutableMetadataItem.builder()
            .dataProduct(dataProduct)
            .component(component)
            .extension("toml")
            .build();

    try (CleanableFileChannel fileChannel = fileApi.fileApi.openForWrite(query)) {
      parameterDataWriter.write(fileChannel, component, samples);
    }
  }

  public NumericalArray readArray(String dataProduct, String component) {
    throw new UnsupportedOperationException();
  }

  public void writeTable(
      String dataProduct, String component, Table<Integer, String, Number> table) {
    throw new UnsupportedOperationException();
  }

  public Table<Integer, String, Number> readTable(String dataProduct, String component) {
    throw new UnsupportedOperationException();
  }

  public void writeArray(String dataProduct, String component, Number[] arr) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    cleanable.clean();
  }
}
