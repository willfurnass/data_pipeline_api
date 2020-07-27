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

  Optional<String> namespace();

  @JsonProperty("run_id")
  Optional<String> runId();

  @JsonProperty("verified_hash")
  Optional<String> verifiedHash();

  Optional<String> calculatedHash();

  Optional<String> source();

  Optional<List<IssueItem>> issues();

  Optional<String> description();

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
            MetadataItem::source,
            metadataItem -> issuesInComparableFormat(metadataItem.issues().orElse(List.of())),
            MetadataItem::namespace,
            MetadataItem::description)
        .stream()
        .map(func -> func.andThen(s -> s.orElse("")))
        .allMatch(func -> keyIsEitherNotPresentOrEqual(func.apply(key), func.apply(this)));
  }

  private Optional<String> issuesInComparableFormat(List<IssueItem> issues) {
    return Optional.of(
        issues.stream().sorted().map(Object::toString).collect(Collectors.joining()));
  }

  private boolean keyIsEitherNotPresentOrEqual(String key, String otherKey) {
    if (key.equals("")) {
      return true;
    }
    return key.equals(otherKey);
  }

  default MetadataItem applyOverrides(List<OverrideItem> overrides) {
    return applyOverrides(overrides, false);
  }

  default MetadataItem applyOverridesIfEmpty(List<OverrideItem> overrides) {
    return applyOverrides(overrides, true);
  }

  private MetadataItem applyOverrides(List<OverrideItem> overrides, boolean onlyOverrideIfEmpty) {
    List<MetadataItem> overridesToApply =
        overrides.stream()
            .filter(overrideItem -> overrideItem.where().map(this::isSuperSetOf).orElse(true))
            .map(OverrideItem::use)
            .flatMap(Optional::stream)
            .collect(Collectors.toList());

    MetadataItem overriddenMetadataItem = ImmutableMetadataItem.copyOf(this);
    for (MetadataItem override : overridesToApply) {
      overriddenMetadataItem = applyOverride(overriddenMetadataItem, override, onlyOverrideIfEmpty);
    }
    return overriddenMetadataItem;
  }

  private boolean shouldOverrideField(
      Function<MetadataItem, Boolean> isFieldPresent,
      MetadataItem baseMetadata,
      MetadataItem overrideMetadata,
      boolean onlyOverrideIfEmpty) {
    boolean isBaseFieldPresent = isFieldPresent.apply(baseMetadata);
    boolean isOverrideFieldPresent = isFieldPresent.apply(overrideMetadata);

    if (!isOverrideFieldPresent) {
      return false;
    }
    if (onlyOverrideIfEmpty) {
      return !isBaseFieldPresent;
    }
    return true;
  }

  private MetadataItem applyOverride(
      MetadataItem baseMetadata, MetadataItem metadataOverride, boolean onlyOverrideIfEmpty) {
    var newMetadataItem = ImmutableMetadataItem.copyOf(baseMetadata);

    Function<Function<MetadataItem, Boolean>, Boolean> shouldOverride =
        fieldFunc ->
            shouldOverrideField(fieldFunc, baseMetadata, metadataOverride, onlyOverrideIfEmpty);

    Function<MetadataItem, Boolean> isFilenamePresent = meta -> meta.internalFilename().isPresent();
    Function<MetadataItem, Boolean> isComponentPresent = meta -> meta.component().isPresent();
    Function<MetadataItem, Boolean> isDataProductPresent = meta -> meta.dataProduct().isPresent();
    Function<MetadataItem, Boolean> isInternalVersionPresent =
        meta -> meta.internalVersion().isPresent();
    Function<MetadataItem, Boolean> isExtensionPresent = meta -> meta.extension().isPresent();
    Function<MetadataItem, Boolean> isVerifiedHashPresent = meta -> meta.verifiedHash().isPresent();
    Function<MetadataItem, Boolean> isCalculatedHashPresent =
        meta -> meta.calculatedHash().isPresent();
    Function<MetadataItem, Boolean> isRunIdPresent = meta -> meta.runId().isPresent();
    Function<MetadataItem, Boolean> isSourcePresent = meta -> meta.source().isPresent();
    Function<MetadataItem, Boolean> isDataDirectoryPresent =
        meta -> meta.dataDirectory().isPresent();
    Function<MetadataItem, Boolean> isIssuesPresent = meta -> meta.issues().isPresent();

    if (shouldOverride.apply(isFilenamePresent)) {
      newMetadataItem =
          newMetadataItem.withInternalFilename(metadataOverride.internalFilename().get());
    }

    if (shouldOverride.apply(isComponentPresent)) {
      newMetadataItem = newMetadataItem.withComponent(metadataOverride.component().get());
    }

    if (shouldOverride.apply(isDataProductPresent)) {
      newMetadataItem = newMetadataItem.withDataProduct(metadataOverride.dataProduct().get());
    }

    if (shouldOverride.apply(isInternalVersionPresent)) {
      newMetadataItem =
          newMetadataItem.withInternalVersion(metadataOverride.internalVersion().get());
    }

    if (shouldOverride.apply(isExtensionPresent)) {
      newMetadataItem = newMetadataItem.withExtension(metadataOverride.extension().get());
    }

    if (shouldOverride.apply(isVerifiedHashPresent)) {
      newMetadataItem = newMetadataItem.withVerifiedHash(metadataOverride.verifiedHash().get());
    }

    if (shouldOverride.apply(isCalculatedHashPresent)) {
      newMetadataItem = newMetadataItem.withCalculatedHash(metadataOverride.calculatedHash().get());
    }

    if (shouldOverride.apply(isRunIdPresent)) {
      newMetadataItem = newMetadataItem.withRunId(metadataOverride.runId().get());
    }

    if (shouldOverride.apply(isSourcePresent)) {
      newMetadataItem = newMetadataItem.withSource(metadataOverride.source().get());
    }

    if (shouldOverride.apply(isDataDirectoryPresent)) {
      newMetadataItem = newMetadataItem.withDataDirectory(metadataOverride.dataDirectory().get());
    }

    if (shouldOverride.apply(isIssuesPresent)) {
      newMetadataItem = newMetadataItem.withIssues(metadataOverride.issues().get());
    }

    if (metadataOverride.namespace().isPresent()) {
      newMetadataItem = newMetadataItem.withNamespace(metadataOverride.namespace().get());
    }

    if (metadataOverride.description().isPresent()) {
      newMetadataItem = newMetadataItem.withDescription(metadataOverride.description().get());
    }

    return newMetadataItem;
  }
}
