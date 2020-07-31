package uk.ramp.parameters;

import org.apache.commons.math3.random.RandomGenerator;
import org.immutables.value.Value.Auxiliary;

public interface Component extends ReadComponent, WriteComponent {
  @Auxiliary
  RandomGenerator rng();
}
