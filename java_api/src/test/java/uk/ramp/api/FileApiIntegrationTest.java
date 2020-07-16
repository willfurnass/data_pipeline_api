package uk.ramp.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import uk.ramp.hash.Hasher;
import uk.ramp.metadata.ImmutableIssueItem;
import uk.ramp.metadata.ImmutableMetadataItem;
import uk.ramp.metadata.IssueItem;

public class FileApiIntegrationTest {
  private String configPath;
  private String parentPath;
  private String dataDirectoryPath;

  @Before
  public void setUp() throws IOException, URISyntaxException {
    configPath = Paths.get(getClass().getResource("/config.yaml").toURI()).toString();
    parentPath = Path.of(configPath).getParent().toString();
    dataDirectoryPath = Path.of(parentPath, "folder/data").toString();
    Files.deleteIfExists(Path.of(dataDirectoryPath, "exampleWrite.toml"));
    Files.deleteIfExists(Path.of("access-runId.yaml"));
  }

  @Test
  public void testOpenForRead() throws Exception {
    var query = ImmutableMetadataItem.builder().component("example-estimate").build();
    var buffer = ByteBuffer.allocate(16);
    FileApi fileApi = new FileApi(Path.of(configPath));
    fileApi.openForRead(query).read(buffer);
    assertThat(new String(buffer.array()).contains("title = \"TOML Example\"\n"));
  }

  @Test
  public void testOpenForWrite() throws IOException {
    var query = ImmutableMetadataItem.builder().internalFilename("exampleWrite.toml").build();
    var buffer = ByteBuffer.allocate(16);
    buffer.put("testWrite".getBytes());
    buffer.flip();
    FileApi fileApi = new FileApi(Path.of(configPath));
    fileApi.openForWrite(query).write(buffer);
    assertThat(Files.readString(Path.of(dataDirectoryPath, "exampleWrite.toml")))
        .isEqualTo("testWrite");
  }

  @Test
  public void testClose() throws IOException {
    FileApi api = new FileApi(Path.of(configPath));
    api.close();
    assertThat(Files.readString(Path.of(parentPath, "access-runId.yaml")))
        .contains("open_timestamp")
        .contains("close_timestamp")
        .contains("run_id")
        .contains("io");
  }

  @Test
  public void testWriteNewHash() throws IOException {
    var writeFilePath = Path.of(dataDirectoryPath, "exampleWrite.toml").toString();
    var query = ImmutableMetadataItem.builder().internalFilename("exampleWrite.toml").build();
    var buffer = ByteBuffer.allocate(16).put("testWrite".getBytes()).flip();
    FileApi fileApi = new FileApi(Path.of(configPath));
    var fileHandle = fileApi.openForWrite(query);
    fileHandle.write(buffer);
    fileHandle.close();
    fileApi.close();

    var calculatedHash = new Hasher().fileHash(writeFilePath);

    assertThat(Files.readString(Path.of(parentPath, "access-runId.yaml")))
        .contains(String.format("calculatedHash: \"%s\"", calculatedHash));
  }

  @Test
  public void testNoIOIfFileHandleNotClosed() throws IOException {
    var query = ImmutableMetadataItem.builder().internalFilename("exampleWrite.toml").build();
    var buffer = ByteBuffer.allocate(16).put("testWrite".getBytes()).flip();
    FileApi fileApi = new FileApi(Path.of(configPath));
    var fileHandle = fileApi.openForWrite(query);
    fileHandle.write(buffer);
    fileApi.close();

    assertThat(Files.readString(Path.of(parentPath, "access-runId.yaml"))).contains("io: []");
  }

  @Test
  public void testIssuesAndDescriptionIsWritten() throws IOException {
    List<IssueItem> issues =
        List.of(ImmutableIssueItem.builder().severity(1).description("issue description").build());
    var query =
        ImmutableMetadataItem.builder()
            .internalFilename("exampleWrite.toml")
            .issues(issues)
            .build();
    var buffer = ByteBuffer.allocate(16).put("testWrite".getBytes()).flip();
    FileApi fileApi = new FileApi(Path.of(configPath));
    var fileHandle = fileApi.openForWrite(query);
    fileHandle.write(buffer);
    fileHandle.close();
    fileApi.close();

    assertThat(Files.readString(Path.of(parentPath, "access-runId.yaml")))
        .contains(
            "    issues:\n" + "    - description: \"issue description\"\n" + "      severity: 1");
  }
}
