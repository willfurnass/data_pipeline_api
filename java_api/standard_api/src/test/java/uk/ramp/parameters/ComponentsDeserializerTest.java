package uk.ramp.parameters;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import uk.ramp.distribution.Distribution.DistributionType;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.samples.ImmutableSamples;

public class ComponentsDeserializerTest {
  private final String json =
      "{\n"
          + "  \"example-distribution\": {\n"
          + "    \"distribution\": \"gamma\",\n"
          + "    \"scale\": 2,\n"
          + "    \"shape\": 1,\n"
          + "    \"type\": \"distribution\"\n"
          + "  },\n"
          + "  \"example-estimate\": {\n"
          + "    \"type\": \"point-estimate\",\n"
          + "    \"value\": 1.0\n"
          + "  },\n"
          + "  \"example-samples\": {\n"
          + "    \"samples\": [\n"
          + "      1,\n"
          + "      2,\n"
          + "      3\n"
          + "    ],\n"
          + "    \"type\": \"samples\"\n"
          + "  }\n"
          + "}";

  private ObjectMapper objectMapper;

  @Before
  public void setUp() throws Exception {
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new GuavaModule());
  }

  @Test
  public void deserialize() throws JsonProcessingException {
    Components actualComponents = objectMapper.readValue(json, Components.class);

    var estimate = ImmutableEstimate.builder().internalValue(1.0).build();
    var distribution =
        ImmutableDistribution.builder()
            .internalType(DistributionType.gamma)
            .internalShape(1)
            .internalScale(2)
            .build();
    var samples = ImmutableSamples.builder().addSamples(1, 2, 3).build();
    var expectedComponents =
        ImmutableComponents.builder()
            .components(
                Map.of(
                    "example-estimate",
                    estimate,
                    "example-distribution",
                    distribution,
                    "example-samples",
                    samples))
            .build();

    assertThat(actualComponents).isEqualTo(expectedComponents);
  }
}
