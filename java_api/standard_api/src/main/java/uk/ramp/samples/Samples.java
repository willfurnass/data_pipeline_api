package uk.ramp.samples;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value.Immutable;
import uk.ramp.distribution.Distribution;
import uk.ramp.distribution.Distribution.DistributionType;
import uk.ramp.distribution.ImmutableDistribution;
import uk.ramp.parameters.Component;

@Immutable
@JsonSerialize
@JsonDeserialize
public interface Samples extends Component {
  List<Number> samples();

  @JsonIgnore
  default Number mean() {
    return samples().stream().mapToDouble(Number::doubleValue).average().orElseThrow();
  }

  @Override
  @JsonIgnore
  default Number getEstimate() {
    return mean();
  }

  @Override
  @JsonIgnore
  default List<Number> getSamples() {
    return samples();
  }

  @Override
  @JsonIgnore
  default Distribution getDistribution() {
    return ImmutableDistribution.builder()
        .internalType(DistributionType.empirical)
        .empiricalSamples(samples())
        .build();
  }
}
