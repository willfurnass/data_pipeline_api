package uk.ramp.toml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class TOMLMapper extends ObjectMapper {
  public TOMLMapper() {
    super(new TOMLFactory());
    this.registerModule(new Jdk8Module());
    this.registerModule(new GuavaModule());
  }
}
