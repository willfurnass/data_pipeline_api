package uk.ramp.yaml;

public class YamlFactory {
  public YamlReader yamlReader() {
    return new BaseYamlReader();
  }

  public YamlWriter yamlWriter() {
    return new BaseYamlWriter();
  }
}
