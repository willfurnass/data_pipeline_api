package uk.ramp.toml;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.OutputDecorator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.moandjiezana.toml.TomlWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class TOMLOutputDecorator extends OutputDecorator {
  @Override
  public OutputStream decorate(IOContext ctxt, final OutputStream out) throws IOException {
    var objMapper =
        new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new GuavaModule());

    return new java.io.ByteArrayOutputStream() {
      @Override
      public void close() throws IOException {
        super.close();
        var jsonStr = new String(this.toByteArray(), StandardCharsets.UTF_8);
        var jsonMap = objMapper.readValue(jsonStr, new TypeReference<Map<String, Object>>() {});
        var tomlStr = new TomlWriter().write(jsonMap);

        out.write(tomlStr.getBytes(StandardCharsets.UTF_8));
        out.flush();
        out.close();
      }
    };
  }

  @Override
  public Writer decorate(IOContext ctxt, final Writer w) throws IOException {
    var objMapper =
        new ObjectMapper().registerModule(new Jdk8Module()).registerModule(new GuavaModule());

    return new java.io.StringWriter() {
      @Override
      public void close() throws IOException {
        super.close();
        String jsonStr = this.toString();
        var jsonMap = objMapper.readValue(jsonStr, new TypeReference<Map<String, Object>>() {});
        var tomlStr = new TomlWriter().write(jsonMap);

        w.write(tomlStr);
        w.close();
      }
    };
  }
}
