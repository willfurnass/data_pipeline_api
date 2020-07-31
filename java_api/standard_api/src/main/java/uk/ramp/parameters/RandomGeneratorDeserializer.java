package uk.ramp.parameters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.math3.random.RandomGenerator;

public class RandomGeneratorDeserializer extends JsonDeserializer<RandomGenerator> {
  private final RandomGenerator rng;

  public RandomGeneratorDeserializer(RandomGenerator rng) {
    this.rng = rng;
  }

  @Override
  public RandomGenerator deserialize(JsonParser p, DeserializationContext ctxt) {
    return rng;
  }
}
