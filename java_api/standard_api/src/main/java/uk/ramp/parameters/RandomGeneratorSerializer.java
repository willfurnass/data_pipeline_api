package uk.ramp.parameters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.apache.commons.math3.random.RandomGenerator;

public class RandomGeneratorSerializer extends JsonSerializer<RandomGenerator> {
  @Override
  public void serialize(RandomGenerator value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    // Intentionally writing a placeholder string here, which is later removed when finalizing
    // serialization
    gen.writeRawValue("placeholder");
  }
}
