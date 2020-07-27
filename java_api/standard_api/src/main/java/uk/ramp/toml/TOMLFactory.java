package uk.ramp.toml;

import com.fasterxml.jackson.databind.MappingJsonFactory;

class TOMLFactory extends MappingJsonFactory {

  TOMLFactory() {
    this._inputDecorator = new TOMLInputDecorator();
    this._outputDecorator = new TOMLOutputDecorator();
  }
}
