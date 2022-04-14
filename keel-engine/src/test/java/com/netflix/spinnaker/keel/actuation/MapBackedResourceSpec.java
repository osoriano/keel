package com.netflix.spinnaker.keel.actuation;

import java.util.Map;
import com.netflix.spinnaker.keel.api.ResourceSpec;
import org.jetbrains.annotations.NotNull;

public class MapBackedResourceSpec implements ResourceSpec {
  private String id;
  private Map<String, Object> data;

  public MapBackedResourceSpec(String id, Map<String, Object> data) {
    this.id = id;
    this.data = data;
  }

  public Map<String, Object> getData() {
    return data;
  }

  @NotNull @Override public String generateId(@NotNull Map<String, ?> metadata) {
    return id;
  }
}
