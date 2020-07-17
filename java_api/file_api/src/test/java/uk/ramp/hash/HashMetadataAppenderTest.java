package uk.ramp.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import uk.ramp.metadata.ImmutableMetadataItem;

public class HashMetadataAppenderTest {
  private Hasher hasher;

  @Before
  public void setUp() {
    this.hasher = mock(Hasher.class);
  }

  @Test
  public void testOpenForRead() {
    var query =
        ImmutableMetadataItem.builder()
            .dataDirectory("dataDirectory")
            .internalFilename("file1")
            .verifiedHash("hash1")
            .build();

    when(hasher.fileHash("dataDirectory/file1")).thenReturn("hash1");

    var hashAppender = new HashMetadataAppender(hasher);

    assertThat(hashAppender.addHash(query, true)).isEqualTo(query.withCalculatedHash("hash1"));
  }

  @Test
  public void testInvalidHash() {
    var query =
        ImmutableMetadataItem.builder()
            .dataDirectory("dataDirectory")
            .internalFilename("file1")
            .verifiedHash("hash1")
            .build();

    when(hasher.fileHash("dataDirectory/file1")).thenReturn("invalidHash");

    var hashAppender = new HashMetadataAppender(hasher);

    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> hashAppender.addHash(query, true));
  }
}
