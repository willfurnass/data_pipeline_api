package uk.ramp.metadata;

import java.util.Map;

public interface WriteOnlyRunMetadata {
  void update(Map<String, String> newData);
}
