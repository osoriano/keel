package com.netflix.spinnaker.keel.jackson.mixins

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.netflix.spinnaker.keel.api.Resource

@JsonPropertyOrder("apiVersion", "name", "application", "metadata", "serviceAccount", "updatedAt", "artifacts", "environments", "previewEnvironments", "rawConfig")
interface DeliveryConfigMixin {
  @get:JsonIgnore
  val resources: List<Resource<*>>
}
