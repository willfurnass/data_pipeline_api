package uk.ramp.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class MetadataItemTest {

  @Test
  public void testIsSuperSetOfWithEqualFilename() {
    var meta1 = ImmutableMetadataItem.builder().internalFilename("filename").build();
    var meta2 = ImmutableMetadataItem.builder().internalFilename("filename").build();

    assertThat(meta1.isSuperSetOf(meta2)).isTrue();
  }

  @Test
  public void testIsSuperSetOfWithEqualDataProduct() {
    var meta1 = ImmutableMetadataItem.builder().dataProduct("dataProduct").build();
    var meta2 = ImmutableMetadataItem.builder().dataProduct("dataProduct").build();

    assertThat(meta1.isSuperSetOf(meta2)).isTrue();
  }

  @Test
  public void testIsSuperSetOfWithEqualExtension() {
    var meta1 = ImmutableMetadataItem.builder().extension("ext").build();
    var meta2 = ImmutableMetadataItem.builder().extension("ext").build();

    assertThat(meta1.isSuperSetOf(meta2)).isTrue();
  }

  @Test
  public void testIsSuperSetOfWithEqualVersion() {
    var meta1 = ImmutableMetadataItem.builder().internalVersion("version").build();
    var meta2 = ImmutableMetadataItem.builder().internalVersion("version").build();

    assertThat(meta1.isSuperSetOf(meta2)).isTrue();
  }

  @Test
  public void testIsSuperSetOfPartialVersion() {
    var queryKey = ImmutableMetadataItem.builder().build();
    var otherKey = ImmutableMetadataItem.builder().internalVersion("version").build();

    assertThat(otherKey.isSuperSetOf(queryKey)).isTrue();
    assertThat(queryKey.isSuperSetOf(otherKey)).isFalse();
  }

  @Test
  public void testIsSuperSetOfPartialFilename() {
    var queryKey = ImmutableMetadataItem.builder().build();
    var otherKey = ImmutableMetadataItem.builder().internalFilename("filename").build();

    assertThat(otherKey.isSuperSetOf(queryKey)).isTrue();
    assertThat(queryKey.isSuperSetOf(otherKey)).isFalse();
  }

  @Test
  public void testIsSuperSetOfPartialFilenameAndVersion() {
    var queryKey = ImmutableMetadataItem.builder().build();
    var otherKey =
        ImmutableMetadataItem.builder()
            .internalFilename("filename")
            .internalVersion("version")
            .build();

    assertThat(otherKey.isSuperSetOf(queryKey)).isTrue();
    assertThat(queryKey.isSuperSetOf(otherKey)).isFalse();
  }

  @Test
  public void testIsSuperSetOfFilenameAndVersionNotMatching() {
    var queryKey = ImmutableMetadataItem.builder().internalFilename("filename").build();
    var otherKey = ImmutableMetadataItem.builder().internalVersion("version").build();

    assertThat(otherKey.isSuperSetOf(queryKey)).isFalse();
    assertThat(queryKey.isSuperSetOf(otherKey)).isFalse();
  }
}
