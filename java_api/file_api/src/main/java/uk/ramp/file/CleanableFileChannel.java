package uk.ramp.file;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Wrapper around standard FileChannel with additional safety net for closing if user does not close
 * and ability to run custom function during cleanup/closing. Users should either use with
 * try-with-resources or ensure they manually call .close() once finished. As a safety net, .close()
 * will be called by a cleaner if and when the instance of CleanableFileChannel is going to be
 * collected by GC.
 */
public class CleanableFileChannel
    implements AutoCloseable, SeekableByteChannel, GatheringByteChannel, ScatteringByteChannel {
  private static final Cleaner cleaner = Cleaner.create(); // safety net for closing
  private final Cleanable cleanable;

  private final FileChannelWrapper fileChannelWrapper;

  public CleanableFileChannel(FileChannel fileChannel, Runnable runOnClose) {
    this.fileChannelWrapper = new FileChannelWrapper(fileChannel, runOnClose);
    this.cleanable = cleaner.register(this, fileChannelWrapper);
  }

  // Defining a resource that requires cleaning
  private static class FileChannelWrapper implements Runnable {
    private final FileChannel fileChannel;
    private final Runnable runOnClose;

    FileChannelWrapper(FileChannel fileChannel, Runnable runOnClose) {
      this.fileChannel = fileChannel;
      this.runOnClose = runOnClose;
    }

    // Invoked by close method or cleaner
    @Override
    public void run() {
      runOnClose.run();
      try {
        fileChannel.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return fileChannelWrapper.fileChannel.read(dst);
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    return fileChannelWrapper.fileChannel.read(dsts, offset, length);
  }

  @Override
  public long read(ByteBuffer[] dsts) throws IOException {
    return fileChannelWrapper.fileChannel.read(dsts);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return fileChannelWrapper.fileChannel.write(src);
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    return fileChannelWrapper.fileChannel.write(srcs, offset, length);
  }

  @Override
  public long write(ByteBuffer[] srcs) throws IOException {
    return fileChannelWrapper.fileChannel.write(srcs);
  }

  @Override
  public long position() throws IOException {
    return fileChannelWrapper.fileChannel.position();
  }

  @Override
  public CleanableFileChannel position(long newPosition) throws IOException {
    var fileChannel = fileChannelWrapper.fileChannel.position(newPosition);

    // Although it is not officially documented, the underlying implementation of position() can
    // return null, so we return null here if underlying implementation returns null.
    if (fileChannel == null) {
      return null;
    }

    return this;
  }

  @Override
  public long size() throws IOException {
    return fileChannelWrapper.fileChannel.size();
  }

  @Override
  public CleanableFileChannel truncate(long size) throws IOException {
    var fileChannel = fileChannelWrapper.fileChannel.truncate(size);

    // Although it is not officially documented, the underlying implementation of truncate() can
    // return null, so we return null here if underlying implementation returns null.
    if (fileChannel == null) {
      return null;
    }

    return this;
  }

  public void force(boolean metaData) throws IOException {
    fileChannelWrapper.fileChannel.force(metaData);
  }

  public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    return fileChannelWrapper.fileChannel.transferTo(position, count, target);
  }

  public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
    return fileChannelWrapper.fileChannel.transferFrom(src, position, count);
  }

  public int read(ByteBuffer dst, long position) throws IOException {
    return fileChannelWrapper.fileChannel.read(dst, position);
  }

  public int write(ByteBuffer src, long position) throws IOException {
    return fileChannelWrapper.fileChannel.write(src, position);
  }

  public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
    return fileChannelWrapper.fileChannel.map(mode, position, size);
  }

  public FileLock lock(long position, long size, boolean shared) throws IOException {
    return fileChannelWrapper.fileChannel.lock(position, size, shared);
  }

  public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    return fileChannelWrapper.fileChannel.tryLock(position, size, shared);
  }

  @Override
  public boolean isOpen() {
    return fileChannelWrapper.fileChannel.isOpen();
  }

  @Override
  public void close() {
    cleanable.clean();
  }
}
