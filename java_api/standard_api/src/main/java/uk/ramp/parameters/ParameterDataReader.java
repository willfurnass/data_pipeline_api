package uk.ramp.parameters;

import uk.ramp.file.CleanableFileChannel;

public interface ParameterDataReader {

  ReadComponent read(CleanableFileChannel fileChannel, String component);
}
