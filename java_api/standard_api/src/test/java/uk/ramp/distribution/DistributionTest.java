package uk.ramp.distribution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;
import uk.ramp.distribution.Distribution.DistributionType;

public class DistributionTest {
  private final Distribution distribution =
      ImmutableDistribution.builder()
          .internalType(DistributionType.gamma)
          .internalShape(1)
          .internalScale(2)
          .build();

  @Test
  public void derivedEstimateFromDistribution() {
    assertThat(distribution.getEstimate().floatValue()).isEqualTo(2F);
  }

  @Test
  public void derivedSamplesFromDistribution() {
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(distribution::getSamples);
  }

  @Test
  public void derivedDistributionFromDistribution() {
    assertThat(distribution.getDistribution()).isEqualTo(distribution);
  }
}
