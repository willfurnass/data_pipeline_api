package uk.ramp.hash;

import java.time.Instant;
import uk.ramp.file.FileReader;

public class Hasher {
  public String fileHash(String fileName) {
    return new Sha1Hasher().hash(new FileReader().read(fileName));
  }

  public String fileHash(String fileName, Instant openTimestamp) {
    return new Sha1Hasher().hash(new FileReader().read(fileName) + openTimestamp.toString());
  }
}
