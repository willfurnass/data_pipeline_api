package uk.ramp.toml;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.InputDecorator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.moandjiezana.toml.Toml;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class TOMLInputDecorator extends InputDecorator {

  @Override
  public InputStream decorate(IOContext ctxt, InputStream in) throws IOException {
    var objectMapper =
        new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new GuavaModule());

    Map<String, Object> map = new Toml().read(in).toMap();
    String objString = objectMapper.writeValueAsString(map);
    return new ByteArrayInputStream(objString.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public InputStream decorate(IOContext ctxt, byte[] src, int offset, int length)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(src, offset, length);
    return decorate(ctxt, in);
  }

  @Override
  public Reader decorate(IOContext ctxt, Reader r) throws IOException {
    var objectMapper =
        new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new GuavaModule());

    Map<String, Object> map = new Toml().read(r).toMap();
    String objString = objectMapper.writeValueAsString(map);
    return new StringReader(objString);
  }
}
