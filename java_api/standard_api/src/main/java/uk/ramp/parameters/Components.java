package uk.ramp.parameters;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.immutables.value.Value.Immutable;

@Immutable
@JsonSerialize(using = ComponentsSerializer.class)
@JsonDeserialize(using = ComponentsDeserializer.class)
public interface Components {
  Map<String, Component> components();
}
