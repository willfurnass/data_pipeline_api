package uk.ramp.metadata;

import static uk.ramp.file.FileDirectoryNormaliser.normalisePath;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import uk.ramp.config.Config.OverrideItem;

@Immutable
@JsonSerialize
@JsonDeserialize
public interface MetadataItem {
  @JsonProperty("filename")
  Optional<String> internalFilename();

  @JsonIgnore
  Optional<String> dataDirectory();

  @JsonIgnore
  default String normalisedFilename() {
    return normalisePath(dataDirectory().orElseThrow(), internalFilename().orElseThrow());
  }

  @JsonProperty("version")
  Optional<String> internalVersion();

  @Derived
  @JsonIgnore
  default ArtifactVersion comparableVersion() {
    return internalVersion()
        .map(DefaultArtifactVersion::new)
        .orElse(new DefaultArtifactVersion("0"));
  }

  Optional<String> extension();

  Optional<String> component();

  @JsonProperty("data_product")
  Optional<String> dataProduct();

  @JsonProperty("run_id")
  Optional<String> runId();

  @JsonProperty("verified_hash")
  Optional<String> verifiedHash();

  Optional<String> calculatedHash();

  Optional<String> source();

  default boolean isSuperSetOf(MetadataItem key) {
    return List.<Function<MetadataItem, Optional<String>>>of(
            MetadataItem::internalFilename,
            MetadataItem::component,
            MetadataItem::dataProduct,
            MetadataItem::internalVersion,
            MetadataItem::extension,
            MetadataItem::verifiedHash,
            MetadataItem::calculatedHash,
            MetadataItem::runId,
            MetadataItem::source)
        .stream()
        .map(func -> func.andThen(s -> s.orElse("")))
        .allMatch(func -> keyIsEitherNotPresentOrEqual(func.apply(key), func.apply(this)));
  }

  private boolean keyIsEitherNotPresentOrEqual(String key, String otherKey) {
    if (key.equals("")) {
      return true;
    }
    return key.equals(otherKey);
  }

  default MetadataItem applyOverrides(List<OverrideItem> overrides) {
    List<MetadataItem> overridesToApply =
        overrides.stream()
            .filter(overrideItem -> overrideItem.where().map(this::isSuperSetOf).orElse(true))
            .map(OverrideItem::use)
            .flatMap(Optional::stream)
            .collect(Collectors.toList());

    MetadataItem overriddenMetadataItem = ImmutableMetadataItem.copyOf(this);
    for (MetadataItem override : overridesToApply) {
      overriddenMetadataItem = applyOverride(overriddenMetadataItem, override);
    }
    return overriddenMetadataItem;
  }

  private MetadataItem applyOverride(MetadataItem baseMetadata, MetadataItem metadataOverride) {
    var newMetadataItem = ImmutableMetadataItem.copyOf(baseMetadata);

    if (metadataOverride.internalFilename().isPresent()) {
      newMetadataItem =
          newMetadataItem.withInternalFilename(metadataOverride.internalFilename().get());
    }

    if (metadataOverride.component().isPresent()) {
      newMetadataItem = newMetadataItem.withComponent(metadataOverride.component().get());
    }

    if (metadataOverride.dataProduct().isPresent()) {
      newMetadataItem = newMetadataItem.withDataProduct(metadataOverride.dataProduct().get());
    }

    if (metadataOverride.internalVersion().isPresent()) {
      newMetadataItem =
          newMetadataItem.withInternalVersion(metadataOverride.internalVersion().get());
    }

    if (metadataOverride.extension().isPresent()) {
      newMetadataItem = newMetadataItem.withExtension(metadataOverride.extension().get());
    }

    if (metadataOverride.verifiedHash().isPresent()) {
      newMetadataItem = newMetadataItem.withVerifiedHash(metadataOverride.verifiedHash().get());
    }

    if (metadataOverride.calculatedHash().isPresent()) {
      newMetadataItem = newMetadataItem.withCalculatedHash(metadataOverride.calculatedHash().get());
    }

    if (metadataOverride.runId().isPresent()) {
      newMetadataItem = newMetadataItem.withRunId(metadataOverride.runId().get());
    }

    if (metadataOverride.source().isPresent()) {
      newMetadataItem = newMetadataItem.withSource(metadataOverride.source().get());
    }

    if (metadataOverride.dataDirectory().isPresent()) {
      newMetadataItem = newMetadataItem.withDataDirectory(metadataOverride.dataDirectory().get());
    }

    return newMetadataItem;
  }
}
