package uk.ramp.metadata;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import uk.ramp.yaml.YamlReader;

public class MetadataSelectorFactory {
  public MetadataSelector metadataSelector(YamlReader yamlReader, String dataDirectory) {
    List<MetadataItem> metadataItems =
        Stream.of(new MetaDataReader(yamlReader, dataDirectory).read())
            .flatMap(Collection::stream)
            .map(i -> i.withDataDirectory(dataDirectory))
            .collect(Collectors.toList());
    return new MatchingMetadataSelector(metadataItems);
  }
}
