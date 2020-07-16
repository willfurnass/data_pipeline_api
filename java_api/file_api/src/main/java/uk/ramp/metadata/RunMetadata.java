package uk.ramp.metadata;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

class RunMetadata implements ReadOnlyRunMetadata, WriteOnlyRunMetadata {
  private final SortedMap<String, String> data;

  RunMetadata(SortedMap<String, String> data) {
    this.data = data;
  }

  @Override
  public void update(Map<String, String> newData) {
    data.putAll(newData);
  }

  @Override
  public Map<String, String> read() {
    return Collections.unmodifiableSortedMap(data);
  }
}
