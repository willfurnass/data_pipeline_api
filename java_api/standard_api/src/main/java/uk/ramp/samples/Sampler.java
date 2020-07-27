package uk.ramp.samples;

import java.util.List;
import java.util.Random;

public class Sampler {
  private final Random random;

  public Sampler(Random random) {
    this.random = random;
  }

  public Number sample(List<Number> samples) {
    return samples.get(random.nextInt(samples.size()));
  }

  public static Number sampleFrom(List<Number> samples) {
    return new Sampler(new Random()).sample(samples);
  }
}
