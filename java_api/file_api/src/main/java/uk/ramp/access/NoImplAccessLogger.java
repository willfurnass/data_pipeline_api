package uk.ramp.access;

import uk.ramp.metadata.MetadataItem;

class NoImplAccessLogger implements AccessLogger {
  @Override
  public void logRead(MetadataItem callMetadata, MetadataItem readMetadata) {
    // intentionally not implemented
  }

  @Override
  public void logWrite(MetadataItem callMetadata, MetadataItem writeMetadata) {
    // intentionally not implemented
  }

  @Override
  public void writeAccessEntries() {
    // intentionally not implemented
  }
}
