package uk.ramp.toml;

import org.apache.commons.math3.random.RandomGenerator;
import uk.ramp.mapper.DataPipelineMapper;

public class TOMLMapper extends DataPipelineMapper {
  public TOMLMapper(RandomGenerator rng) {
    super(new TOMLFactory(rng), rng);
  }
}
