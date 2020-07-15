package uk.ramp.file;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;

public class CleanableFileChannelIntegrationTest {

  private FileChannel fileChannelReadable;
  private FileChannel fileChannelWritable;
  private AtomicBoolean runnableExecuted;

  @Before
  public void setUp() throws Exception {
    String parentPath =
        Paths.get(getClass().getResource("/config.yaml").toURI()).getParent().toString();
    this.fileChannelReadable =
        FileChannel.open(Path.of(parentPath, "folder/data/parameter/example1.toml"), READ);
    this.fileChannelWritable =
        FileChannel.open(Path.of(parentPath, "folder/data/parameter/example1.toml"), WRITE);
    runnableExecuted = new AtomicBoolean(false);
  }

  @Test
  public void testRunnableExecutesOnClose() throws IOException {
    var cleanableFileChannel =
        new CleanableFileChannel(fileChannelReadable, () -> runnableExecuted.set(true));
    cleanableFileChannel.close();
    assertThat(runnableExecuted).isTrue();
  }

  @Test
  public void testOpenForRead() throws IOException {
    var cleanableFileChannel =
        new CleanableFileChannel(fileChannelReadable, () -> runnableExecuted.set(true));
    cleanableFileChannel.read(ByteBuffer.allocate(64));
    assertThat(fileChannelReadable.isOpen()).isTrue();
  }

  @Test
  public void testOpenForWrite() throws IOException {
    var cleanableFileChannel =
        new CleanableFileChannel(fileChannelWritable, () -> runnableExecuted.set(true));
    var buffer = ByteBuffer.allocate(64);
    buffer.flip();
    cleanableFileChannel.write(buffer);
    assertThat(fileChannelWritable.isOpen()).isTrue();
  }

  @Test
  public void testReadWithWriteFileHandle() {
    var cleanableFileChannel =
        new CleanableFileChannel(fileChannelWritable, () -> runnableExecuted.set(true));
    assertThatExceptionOfType(NonReadableChannelException.class)
        .isThrownBy(() -> cleanableFileChannel.read(ByteBuffer.allocate(64)));
  }

  @Test
  public void testWriteWithReadFileHandle() {
    var cleanableFileChannel =
        new CleanableFileChannel(fileChannelReadable, () -> runnableExecuted.set(true));
    assertThatExceptionOfType(NonWritableChannelException.class)
        .isThrownBy(() -> cleanableFileChannel.write(ByteBuffer.allocate(64)));
  }
}
