package uk.ramp.toml;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.io.OutputDecorator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.moandjiezana.toml.TomlWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.math3.random.RandomGenerator;
import uk.ramp.mapper.DataPipelineMapper;

class TOMLOutputDecorator extends OutputDecorator {
  private final RandomGenerator rng;

  TOMLOutputDecorator(RandomGenerator rng) {
    this.rng = rng;
  }

  @Override
  public OutputStream decorate(IOContext ctxt, final OutputStream out) throws IOException {
    var objMapper = new DataPipelineMapper(rng);

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
    var objMapper = new DataPipelineMapper(rng);

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
