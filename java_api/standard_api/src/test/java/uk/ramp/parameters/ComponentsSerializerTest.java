package uk.ramp.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.io.StringWriter;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import uk.ramp.distribution.Distribution.DistributionType;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.estimate.ImmutableEstimate;
import uk.ramp.samples.ImmutableSamples;

public class ComponentsSerializerTest {
  private final String expectedJson =
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
  public void serialize() throws IOException, JSONException {
    var writer = new StringWriter();
    var estimate = ImmutableEstimate.builder().internalValue(1.0).build();
    var distribution =
        ImmutableDistribution.builder()
            .internalType(DistributionType.gamma)
            .internalShape(1)
            .internalScale(2)
            .build();
    var samples = ImmutableSamples.builder().addSamples(1, 2, 3).build();
    Components components =
        ImmutableComponents.builder()
            .putComponents("example-estimate", estimate)
            .putComponents("example-distribution", distribution)
            .putComponents("example-samples", samples)
            .build();

    objectMapper.writeValue(writer, components);

    var actualJson = writer.toString();
    assertThat(actualJson).isNotBlank();
    assertEquals(actualJson, expectedJson, true);
  }
}
