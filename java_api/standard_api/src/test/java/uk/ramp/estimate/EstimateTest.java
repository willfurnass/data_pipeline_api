package uk.ramp.estimate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Before;
import org.junit.Test;

public class EstimateTest {
  private RandomGenerator rng;

  @Before
  public void setUp() {
    rng = mock(RandomGenerator.class);
    when(rng.nextDouble()).thenReturn(0D);
  }

  @Test
  public void derivedEstimateFromEstimate() {
    var data = ImmutableEstimate.builder().internalValue(5).rng(rng).build();
    assertThat(data.getEstimate()).isEqualTo(5);
  }

  @Test
  public void derivedSampleFromEstimate() {
    var data = ImmutableEstimate.builder().internalValue(5).rng(rng).build();
    assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(data::getSamples);
  }

  @Test
  public void derivedDistributionFromEstimate() {
    var data = ImmutableEstimate.builder().internalValue(5).rng(rng).build();
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(data::getDistribution);
  }
}
