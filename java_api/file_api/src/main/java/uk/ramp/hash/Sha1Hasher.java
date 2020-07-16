package uk.ramp.hash;

import static org.apache.commons.codec.digest.DigestUtils.sha1Hex;

class Sha1Hasher {
  public String hash(String key) {
    return sha1Hex(key);
  }
}
