package uk.ramp.access;

import uk.ramp.metadata.MetadataItem;

public interface AccessLogger {
  void logRead(MetadataItem callMetadata, MetadataItem readMetadata);

  void logWrite(MetadataItem callMetadata, MetadataItem writeMetadata);

  void writeAccessEntries();
}
