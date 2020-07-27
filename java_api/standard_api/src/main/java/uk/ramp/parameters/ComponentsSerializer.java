package uk.ramp.parameters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.samples.ImmutableSamples;

public class ComponentsSerializer extends JsonSerializer<Components> {
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
    ObjectMapper objectMapper =
        new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new GuavaModule());
    JsonNode componentsJsonNode = objectMapper.valueToTree(component);
    ObjectNode componentsObjectNode = componentsJsonNode.deepCopy();
    componentsObjectNode.put("type", typeMapping.get(component.getClass()));
    try {
      gen.writeObjectField(componentName, componentsObjectNode);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
