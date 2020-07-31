package uk.ramp.distribution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.offset;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well512a;
import org.junit.Before;
import org.junit.Test;
import uk.ramp.distribution.Distribution.DistributionType;

public class DistributionTest {
  private Distribution gammaDistribution;
  private Distribution categoricalDistribution;

  @Before
  public void setUp() {
    RandomGenerator rng = new Well512a();

    gammaDistribution =
        ImmutableDistribution.builder()
            .internalType(DistributionType.gamma)
            .internalShape(1)
            .internalScale(2)
            .rng(rng)
            .build();

    MinMax firstMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(0)
            .upperBoundary(4)
            .build();

    MinMax secondMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(5)
            .upperBoundary(9)
            .build();

    MinMax thirdMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(10)
            .upperBoundary(14)
            .build();

    MinMax fourthMinMax =
        ImmutableMinMax.builder()
            .isLowerInclusive(true)
            .isUpperInclusive(true)
            .lowerBoundary(15)
            .upperBoundary(20)
            .build();

    categoricalDistribution =
        ImmutableDistribution.builder()
            .internalType(DistributionType.categorical)
            .bins(List.of(firstMinMax, secondMinMax, thirdMinMax, fourthMinMax))
            .weights(List.of(0.4, 0.1, 0.1, 0.4))
            .rng(rng)
            .build();
  }

  @Test
  public void derivedEstimateFromDistribution() {
    assertThat(gammaDistribution.getEstimate().floatValue()).isEqualTo(2F);
  }

  @Test
  public void derivedSamplesFromDistribution() {
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(gammaDistribution::getSamples);
  }

  @Test
  public void derivedDistributionFromDistribution() {
    assertThat(gammaDistribution.getDistribution()).isEqualTo(gammaDistribution);
  }

  @Test
  public void categoricalDistributionEstimate() {
    assertThat(categoricalDistribution.getEstimate().doubleValue()).isCloseTo(10D, offset(0.5D));
  }

  @Test
  public void categoricalDistributionSample() {
    double sampleAvg =
        IntStream.range(0, 10000)
            .mapToDouble(i -> categoricalDistribution.getSample().doubleValue())
            .average()
            .orElseThrow();

    assertThat(sampleAvg).isCloseTo(10D, offset(0.5D));
  }

  @Test
  public void categoricalDistributionWeighting() {
    double[] samples =
        IntStream.range(0, 10000)
            .mapToDouble(i -> categoricalDistribution.getSample().doubleValue())
            .toArray();

    long firstBinOccurrences = Arrays.stream(samples).filter(s -> 0 <= s && s < 5).count();
    long secondBinOccurrences = Arrays.stream(samples).filter(s -> 5 <= s && s < 10).count();
    long thirdBinOccurrences = Arrays.stream(samples).filter(s -> 10 <= s && s < 15).count();
    long fourthBinOccurrences = Arrays.stream(samples).filter(s -> 15 <= s && s <= 20).count();

    assertThat(firstBinOccurrences).isCloseTo(4000, withinPercentage(15));
    assertThat(secondBinOccurrences).isCloseTo(1000, withinPercentage(15));
    assertThat(thirdBinOccurrences).isCloseTo(1000, withinPercentage(15));
    assertThat(fourthBinOccurrences).isCloseTo(4000, withinPercentage(15));
  }
}
