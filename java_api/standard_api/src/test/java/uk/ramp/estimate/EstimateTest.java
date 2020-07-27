package uk.ramp.estimate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

public class EstimateTest {
  @Test
  public void derivedEstimateFromEstimate() {
    var data = ImmutableEstimate.builder().internalValue(5).build();
    assertThat(data.getEstimate()).isEqualTo(5);
  }

  @Test
  public void derivedSampleFromEstimate() {
    var data = ImmutableEstimate.builder().internalValue(5).build();
    assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(data::getSamples);
  }

  @Test
  public void derivedDistributionFromEstimate() {
    var data = ImmutableEstimate.builder().internalValue(5).build();
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(data::getDistribution);
  }
}
