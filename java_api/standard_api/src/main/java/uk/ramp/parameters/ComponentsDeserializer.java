package uk.ramp.parameters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.math3.random.RandomGenerator;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.mapper.DataPipelineMapper;
import uk.ramp.samples.ImmutableSamples;

public class ComponentsDeserializer extends JsonDeserializer<Components> {
  private final RandomGenerator rng;

  public ComponentsDeserializer(RandomGenerator rng) {
    this.rng = rng;
  }

  private static final Map<String, Class<?>> typeMapping =
      Map.of(
          "point-estimate", ImmutableEstimate.class,
          "distribution", ImmutableDistribution.class,
          "samples", ImmutableSamples.class);

  @Override
  public Components deserialize(JsonParser jsonParser, DeserializationContext ctxt)
      throws IOException, JsonProcessingException {
    JsonNode rootNode = jsonParser.getCodec().readTree(jsonParser);

    var components =
        Streams.stream(rootNode.fields())
            .map(this::deserializeSingleComponent)
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    return ImmutableComponents.builder().components(components).build();
  }

  private Entry<String, Component> deserializeSingleComponent(Entry<String, JsonNode> entry) {
    String key = entry.getKey();
    ObjectNode componentNode = entry.getValue().deepCopy();
    componentNode.putPOJO("rng", new Object()); // this is a hack to force rng to populate via
    // RandomGeneratorDeserializer.class
    String type = componentNode.get("type").asText();
    componentNode.remove("type");
    Class<?> deserializeClass = typeMapping.get(type);

    if (deserializeClass == null) {
      throw new IllegalArgumentException(String.format("Unsupported component type %s", type));
    }

    ObjectMapper objectMapper = new DataPipelineMapper(rng);
    Component component;
    try {
      component = (Component) objectMapper.treeToValue(componentNode, deserializeClass);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
    return new SimpleImmutableEntry<>(key, component);
  }
}
