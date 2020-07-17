package uk.ramp.access;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value.Immutable;
import uk.ramp.config.Config;

@Immutable
@JsonSerialize
@JsonDeserialize
public interface AccessLog {
  @JsonProperty("data_directory")
  String dataDirectory();

  @JsonProperty("open_timestamp")
  Instant openTimestamp();

  @JsonProperty("close_timestamp")
  Instant closeTimestamp();

  @JsonProperty("run_id")
  String runId();

  Config config();

  @JsonProperty("io")
  List<AccessEntry> accessEntries();

  @JsonProperty("metadata")
  Map<String, String> runMetadata();
}
