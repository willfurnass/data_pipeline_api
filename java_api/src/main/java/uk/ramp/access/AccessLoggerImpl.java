package uk.ramp.access;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import uk.ramp.config.Config;
import uk.ramp.hash.Hasher;
import uk.ramp.metadata.ImmutableMetadataItem;
import uk.ramp.metadata.MetadataItem;

class AccessLoggerImpl implements AccessLogger {
  private final List<AccessEntry> accessEntries;
  private final Clock clock;
  private final Instant openTimestamp;
  private final AccessLogWriter writer;
  private final Config config;
  private final Hasher hasher;

  AccessLoggerImpl(
      List<AccessEntry> accessEntries,
      Clock clock,
      AccessLogWriter writer,
      Config config,
      Instant openTimestamp,
      Hasher hasher) {
    this.accessEntries = accessEntries;
    this.clock = clock;
    this.openTimestamp = openTimestamp;
    this.writer = writer;
    this.config = config;
    this.hasher = hasher;
  }

  @Override
  public void logRead(MetadataItem callMetadata, MetadataItem readMetadata) {
    var accessEntry =
        ImmutableAccessEntry.builder()
            .type("read")
            .timestamp(clock.instant())
            .callMetadata(callMetadata)
            .accessMetadata(readMetadata)
            .build();
    accessEntries.add(accessEntry);
  }

  @Override
  public void logWrite(MetadataItem callMetadata, MetadataItem writeMetadata) {
    var writtenFilePath = writeMetadata.normalisedFilename();
    var newHash = hasher.fileHash(writtenFilePath);
    var overrideWriteMetadata =
        ImmutableMetadataItem.copyOf(writeMetadata).withCalculatedHash(newHash);
    var accessEntry =
        ImmutableAccessEntry.builder()
            .type("write")
            .timestamp(clock.instant())
            .callMetadata(callMetadata)
            .accessMetadata(overrideWriteMetadata)
            .build();
    accessEntries.add(accessEntry);
  }

  @Override
  public void writeAccessEntries() {
    var log =
        ImmutableAccessLog.builder()
            .accessEntries(accessEntries)
            .runId(config.runId().orElseThrow())
            .openTimestamp(openTimestamp)
            .closeTimestamp(clock.instant())
            .dataDirectory(config.normalisedDataDirectory())
            .config(config)
            .build();
    writer.write(log);
  }
}
