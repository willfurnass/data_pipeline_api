package uk.ramp.parameters;

import java.util.List;
import uk.ramp.distribution.Distribution;

public interface ReadComponent {
  Number getEstimate();

  List<Number> getSamples();

  Distribution getDistribution();
}
