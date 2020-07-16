package uk.ramp.metadata;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Comparator;
import org.immutables.value.Value.Immutable;

@Immutable
@JsonSerialize
@JsonDeserialize
@JsonPropertyOrder({"description", "severity"})
public interface IssueItem extends Comparable<IssueItem> {
  String description();

  int severity();

  @Override
  default int compareTo(IssueItem o) {
    return Comparator.comparing(IssueItem::description)
        .thenComparingInt(IssueItem::severity)
        .compare(this, o);
  }
}
