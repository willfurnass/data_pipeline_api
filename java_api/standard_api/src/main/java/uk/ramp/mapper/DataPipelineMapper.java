package uk.ramp.mapper;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.commons.math3.random.RandomGenerator;
import uk.ramp.parameters.Components;
import uk.ramp.parameters.ComponentsDeserializer;
import uk.ramp.parameters.ComponentsSerializer;
import uk.ramp.parameters.RandomGeneratorDeserializer;
import uk.ramp.parameters.RandomGeneratorSerializer;

public class DataPipelineMapper extends ObjectMapper {
  public DataPipelineMapper(MappingJsonFactory mappingJsonFactory, RandomGenerator rng) {
    super(mappingJsonFactory);
    registerInternalModules(rng);
  }

  public DataPipelineMapper(RandomGenerator rng) {
    super();
    registerInternalModules(rng);
  }

  public DataPipelineMapper(JsonFactory jf, RandomGenerator rng) {
    super(jf);
    registerInternalModules(rng);
  }

  private void registerInternalModules(RandomGenerator rng) {
    var rngDeserializeModule =
        new SimpleModule()
            .addDeserializer(RandomGenerator.class, new RandomGeneratorDeserializer(rng));
    var rngSerializeModule =
        new SimpleModule().addSerializer(RandomGenerator.class, new RandomGeneratorSerializer());
    var componentsDeserializeModule =
        new SimpleModule().addDeserializer(Components.class, new ComponentsDeserializer(rng));
    var componentsSerializeModule =
        new SimpleModule().addSerializer(Components.class, new ComponentsSerializer(rng));
    this.registerModule(new Jdk8Module());
    this.registerModule(new GuavaModule());
    this.registerModule(rngDeserializeModule);
    this.registerModule(rngSerializeModule);
    this.registerModule(componentsDeserializeModule);
    this.registerModule(componentsSerializeModule);
    this.setSerializationInclusion(Include.NON_DEFAULT);
  }
}
