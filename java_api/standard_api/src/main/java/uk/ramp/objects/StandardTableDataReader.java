package uk.ramp.objects;

import uk.ramp.file.CleanableFileChannel;

public interface StandardTableDataReader {
  StandardTable readTable(CleanableFileChannel fileChannel, String component);
}
