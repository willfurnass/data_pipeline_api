package uk.ramp.metadata;

import static java.util.Comparator.comparing;

import java.util.List;

public class MatchingMetadataSelector implements MetadataSelector {
  private final List<MetadataItem> metadataItems;

  MatchingMetadataSelector(List<MetadataItem> metadataItems) {
    this.metadataItems = metadataItems;
  }

  @Override
  public MetadataItem find(MetadataItem key) {
    return metadataItems.stream()
        .filter(i -> i.isSuperSetOf(key))
        .max(comparing(MetadataItem::comparableVersion))
        .orElse(key);
  }
}
