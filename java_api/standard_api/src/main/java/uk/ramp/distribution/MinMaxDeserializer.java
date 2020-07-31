package uk.ramp.distribution;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;

public class MinMaxDeserializer extends JsonDeserializer<MinMax> {
  private static final String INCLUSIVE_LEFT = "[";
  private static final String EXCLUSIVE_LEFT = "(";
  private static final String INCLUSIVE_RIGHT = "]";
  private static final String EXCLUSIVE_RIGHT = ")";
  private static final String SEPARATOR = ",";

  private MinMax deserialize(String serializedStr) {
    boolean lowerInclusive;
    boolean upperInclusive;
    if (serializedStr.startsWith(INCLUSIVE_LEFT)) {
      lowerInclusive = true;
    } else if (serializedStr.startsWith(EXCLUSIVE_LEFT)) {
      lowerInclusive = false;
    } else {
      throw new IllegalArgumentException(String.format("%s has unexpected format.", serializedStr));
    }

    if (serializedStr.endsWith(INCLUSIVE_RIGHT)) {
      upperInclusive = true;
    } else if (serializedStr.endsWith(EXCLUSIVE_RIGHT)) {
      upperInclusive = false;
    } else {
      throw new IllegalArgumentException(String.format("%s has unexpected format.", serializedStr));
    }

    String strippedBoundaries = serializedStr.replace(INCLUSIVE_LEFT, "");
    strippedBoundaries = strippedBoundaries.replace(INCLUSIVE_RIGHT, "");
    strippedBoundaries = strippedBoundaries.replace(EXCLUSIVE_LEFT, "");
    strippedBoundaries = strippedBoundaries.replace(EXCLUSIVE_RIGHT, "");

    String[] lowerAndUpperBoundary = strippedBoundaries.split(SEPARATOR);

    if (lowerAndUpperBoundary.length != 2) {
      throw new IllegalArgumentException(String.format("%s has unexpected format.", serializedStr));
    }

    int lower = Integer.parseInt(lowerAndUpperBoundary[0]);
    int upper = Integer.parseInt(lowerAndUpperBoundary[1]);

    return ImmutableMinMax.builder()
        .lowerBoundary(lower)
        .isLowerInclusive(lowerInclusive)
        .upperBoundary(upper)
        .isUpperInclusive(upperInclusive)
        .build();
  }

  @Override
  public MinMax deserialize(JsonParser p, DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    String serializedStr = p.readValueAs(String.class);
    return deserialize(serializedStr);
  }
}
