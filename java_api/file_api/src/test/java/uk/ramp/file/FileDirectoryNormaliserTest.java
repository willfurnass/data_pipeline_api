package uk.ramp.file;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class FileDirectoryNormaliserTest {
  private String parentPath;

  @Before
  public void setUp() throws Exception {
    parentPath = "parentPath/";
  }

  @Test
  public void normaliseAbsolutePath() {
    var normaliser = new FileDirectoryNormaliser(parentPath);
    assertThat(normaliser.normalisePath("/file.txt")).isEqualTo("/file.txt");
  }

  @Test
  public void normaliseRelativePath() {
    var normaliser = new FileDirectoryNormaliser(parentPath);

    assertThat(normaliser.normalisePath("file.txt")).isEqualTo("parentPath/file.txt");
  }
}
