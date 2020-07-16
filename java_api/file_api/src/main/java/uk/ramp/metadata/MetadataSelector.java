package uk.ramp.metadata;

public interface MetadataSelector {
  MetadataItem find(MetadataItem query);
}
