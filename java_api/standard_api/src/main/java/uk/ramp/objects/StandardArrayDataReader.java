package uk.ramp.objects;

import uk.ramp.file.CleanableFileChannel;

public interface StandardArrayDataReader {
  NumericalArray read(CleanableFileChannel fileChannel, String component);
}
