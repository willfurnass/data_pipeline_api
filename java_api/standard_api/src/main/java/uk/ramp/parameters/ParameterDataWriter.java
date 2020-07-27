package uk.ramp.parameters;

import uk.ramp.file.CleanableFileChannel;

public interface ParameterDataWriter {

  void write(CleanableFileChannel fileChannel, String component, Component data);
}
