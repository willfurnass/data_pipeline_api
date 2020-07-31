package uk.ramp.samples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static uk.ramp.distribution.Distribution.DistributionType.empirical;

import java.util.stream.IntStream;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well512a;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SamplesTest {
  private RandomGenerator rng;

  @Before
  public void setUp() {
    rng = new Well512a();
  }

  @Test
  public void derivedEstimateFromSamples() {
    var samples = ImmutableSamples.builder().addSamples(1, 2, 3).rng(rng).build();
    assertThat(samples.getEstimate().floatValue()).isCloseTo(2, offset(1e-7F));
  }

  @Test
  @Ignore
  // TODO - large numbers are currently unsupported.
  public void derivedEstimateLargeSamples() {
    var largeValue = 100_000_000_000_000_000L;
    var samples =
        ImmutableSamples.builder().addSamples(largeValue, largeValue + 1, largeValue + 2).build();
    assertThat(samples.getEstimate()).isEqualTo(largeValue + 1);
  }

  @Test
  public void derivedSamplesFromSamples() {
    var samples = ImmutableSamples.builder().addSamples(1, 2, 3).rng(rng).build();
    assertThat(samples.getSamples()).containsExactly(1, 2, 3);
  }

  @Test
  public void derivedDistributionFromSamples() {
    var samples = ImmutableSamples.builder().addSamples(1, 2, 3).rng(rng).build();
    var distribution = samples.getDistribution();
    assertThat(distribution.internalType()).isEqualTo(empirical);
    assertThat(distribution.getEstimate().floatValue()).isCloseTo(2, offset(1e-7F));
    var distSampleAvg =
        IntStream.range(0, 10000)
            .parallel()
            .mapToDouble(i -> distribution.getSample().doubleValue())
            .average()
            .orElseThrow();
    assertThat(distSampleAvg).isBetween(1.95, 2.05);
  }
}
