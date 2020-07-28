package uk.ramp.distribution;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

@JsonSerialize(using = MinMaxSerializer.class)
@JsonDeserialize(using = MinMaxDeserializer.class)
@Immutable
public interface MinMax {
  int lowerBoundary();

  boolean isLowerInclusive();

  int upperBoundary();

  boolean isUpperInclusive();

  @Derived
  default int lowerInclusive() {
    return isLowerInclusive() ? lowerBoundary() : lowerBoundary() + 1;
  }

  @Derived
  default int upperIncluive() {
    return isUpperInclusive() ? upperBoundary() : upperBoundary() - 1;
  }

  @Derived
  default int lowerExclusive() {
    return isLowerInclusive() ? lowerBoundary() + 1 : lowerBoundary();
  }

  @Derived
  default int upperExclusive() {
    return isUpperInclusive() ? upperBoundary() - 1 : upperBoundary();
  }
}
