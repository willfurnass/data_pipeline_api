package uk.ramp.access;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import org.immutables.value.Value.Immutable;
import uk.ramp.metadata.MetadataItem;

@Immutable
@JsonSerialize
@JsonDeserialize
public interface AccessEntry {
  String type();

  Instant timestamp();

  MetadataItem callMetadata();

  MetadataItem accessMetadata();
}
