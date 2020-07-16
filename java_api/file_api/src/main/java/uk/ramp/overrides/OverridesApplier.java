package uk.ramp.overrides;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import uk.ramp.config.Config;
import uk.ramp.config.Config.OverrideItem;
import uk.ramp.config.ImmutableOverrideItem;
import uk.ramp.metadata.ImmutableMetadataItem;
import uk.ramp.metadata.MetadataItem;

public class OverridesApplier {
  private final Config config;

  public OverridesApplier(Config config) {
    this.config = config;
  }

  public MetadataItem applyWriteOverrides(MetadataItem query) {
    OverrideItem runIdOverride =
        ImmutableOverrideItem.builder()
            .use(ImmutableMetadataItem.builder().runId(config.runId().orElseThrow()).build())
            .build();
    Supplier<String> generatedFilename =
        () ->
            Paths.get(
                    query.dataProduct().orElseThrow(),
                    query.runId().orElseThrow() + "." + query.extension().orElseThrow())
                .toString();
    OverrideItem filenameOverride =
        ImmutableOverrideItem.builder()
            .use(
                ImmutableMetadataItem.builder()
                    .internalFilename(query.internalFilename().orElseGet(generatedFilename))
                    .build())
            .build();

    OverrideItem dataDirectoryOverride =
        ImmutableOverrideItem.builder()
            .use(
                ImmutableMetadataItem.builder()
                    .dataDirectory(config.normalisedDataDirectory())
                    .build())
            .build();

    return query
        .applyOverrides(new ArrayList<>(config.writeQueryOverrides()))
        .applyOverrides(List.of(runIdOverride))
        .applyOverrides(List.of(filenameOverride))
        .applyOverrides(List.of(dataDirectoryOverride));
  }

  public MetadataItem applyReadOverrides(MetadataItem query) {
    OverrideItem dataDirectoryOverride =
        ImmutableOverrideItem.builder()
            .use(
                ImmutableMetadataItem.builder()
                    .dataDirectory(config.normalisedDataDirectory())
                    .build())
            .build();

    return query
        .applyOverrides(new ArrayList<>(config.readQueryOverrides()))
        .applyOverrides(List.of(dataDirectoryOverride));
  }
}
