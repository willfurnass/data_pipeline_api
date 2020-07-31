package uk.ramp.toml;

import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.commons.math3.random.RandomGenerator;

class TOMLFactory extends MappingJsonFactory {

  TOMLFactory(RandomGenerator rng) {
    this._inputDecorator = new TOMLInputDecorator(rng);
    this._outputDecorator = new TOMLOutputDecorator(rng);
  }
}
