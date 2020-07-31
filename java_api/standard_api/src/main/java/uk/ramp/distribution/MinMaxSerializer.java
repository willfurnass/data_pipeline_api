package uk.ramp.distribution;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class MinMaxSerializer extends JsonSerializer<MinMax> {
  private static final String INCLUSIVE_LEFT = "[";
  private static final String EXCLUSIVE_LEFT = "(";
  private static final String INCLUSIVE_RIGHT = "]";
  private static final String EXCLUSIVE_RIGHT = ")";
  private static final String SEPARATOR = ",";

  @Override
  public void serialize(MinMax value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    var serializedStr = serialize(value);
    gen.writeString(serializedStr);
  }

  private String serialize(MinMax value) {
    String first = value.isLowerInclusive() ? INCLUSIVE_LEFT : EXCLUSIVE_LEFT;
    String last = value.isUpperInclusive() ? INCLUSIVE_RIGHT : EXCLUSIVE_RIGHT;
    return first + value.lowerBoundary() + SEPARATOR + value.upperBoundary() + last;
  }
}
