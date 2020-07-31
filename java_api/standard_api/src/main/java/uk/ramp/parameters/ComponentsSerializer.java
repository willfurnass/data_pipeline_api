package uk.ramp.parameters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.commons.math3.random.RandomGenerator;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.mapper.DataPipelineMapper;
import uk.ramp.samples.ImmutableSamples;

public class ComponentsSerializer extends JsonSerializer<Components> {
  private final RandomGenerator rng;

  public ComponentsSerializer(RandomGenerator rng) {
    this.rng = rng;
  }

  private static final Map<Class<?>, String> typeMapping =
      Map.of(
          ImmutableEstimate.class, "point-estimate",
          ImmutableDistribution.class, "distribution",
          ImmutableSamples.class, "samples");

  @Override
  public void serialize(Components components, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    components.components().forEach((key, value) -> writeSingleComponent(gen, key, value));
    gen.writeEndObject();
  }

  private void writeSingleComponent(JsonGenerator gen, String componentName, Component component) {
    ObjectMapper objectMapper = new DataPipelineMapper(rng);
    String componentStringType = typeMapping.get(component.getClass());

    if (componentStringType == null) {
      throw new IllegalArgumentException(
          String.format("Unsupported component class %s", component.getClass().getName()));
    }

    JsonNode componentsJsonNode = objectMapper.valueToTree(component);
    ObjectNode componentsObjectNode = componentsJsonNode.deepCopy();
    componentsObjectNode.put("type", componentStringType);
    componentsObjectNode.remove("rng"); // this is a hack to force rng to not populate
    try {
      gen.writeObjectField(componentName, componentsObjectNode);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
