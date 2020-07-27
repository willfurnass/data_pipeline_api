package uk.ramp.api;

import com.google.common.collect.Table;
import java.nio.file.Path;
import java.util.List;
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

public class StandardApi {
  private final FileApi fileApi;
  private final ParameterDataReader parameterDataReader;
  private final ParameterDataWriter parameterDataWriter;

  public StandardApi(Path configPath) {
    this.fileApi = new FileApi(configPath);
    this.parameterDataReader = new ParameterDataReaderImpl(new TomlReader(new TOMLMapper()));
    this.parameterDataWriter = new ParameterDataWriterImpl(new TomlWriter(new TOMLMapper()));
  }

  StandardApi(
      FileApi fileApi,
      ParameterDataReader parameterDataReader,
      ParameterDataWriter parameterDataWriter) {
    this.fileApi = fileApi;
    this.parameterDataReader = parameterDataReader;
    this.parameterDataWriter = parameterDataWriter;
  }

  public Number readEstimate(String dataProduct, String component) {
    var query =
        ImmutableMetadataItem.builder().dataProduct(dataProduct).component(component).build();

    ReadComponent data;
    try (CleanableFileChannel fileChannel = fileApi.openForRead(query)) {
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
    var estimate = ImmutableEstimate.builder().internalValue(estimateNumber).build();

    try (CleanableFileChannel fileChannel = fileApi.openForWrite(query)) {
      parameterDataWriter.write(fileChannel, component, estimate);
    }
  }

  public Distribution readDistribution(String dataProduct, String component) {
    var query =
        ImmutableMetadataItem.builder().dataProduct(dataProduct).component(component).build();

    ReadComponent data;
    try (CleanableFileChannel fileChannel = fileApi.openForRead(query)) {
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

    try (CleanableFileChannel fileChannel = fileApi.openForWrite(query)) {
      parameterDataWriter.write(fileChannel, component, distribution);
    }
  }

  public List<Number> readSamples(String dataProduct, String component) {
    var query =
        ImmutableMetadataItem.builder().dataProduct(dataProduct).component(component).build();

    ReadComponent data;
    try (CleanableFileChannel fileChannel = fileApi.openForRead(query)) {
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

    try (CleanableFileChannel fileChannel = fileApi.openForWrite(query)) {
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
}
