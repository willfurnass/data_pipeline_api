package uk.ramp.file;

import java.nio.file.Path;

public class FileDirectoryNormaliser {
  private final String parentPath;

  FileDirectoryNormaliser(String parentPath) {
    this.parentPath = parentPath;
  }

  protected String normalisePath(String path) {
    if (Path.of(path).isAbsolute()) {
      return path;
    }

    return Path.of(parentPath, path).toString();
  }

  public static String normalisePath(String parentPath, String path) {
    return new FileDirectoryNormaliser(parentPath).normalisePath(path);
  }
}
