package com.netflix.spinnaker.keel.artifacts

import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.FROM_ANY_BRANCH
import com.netflix.spinnaker.keel.api.artifacts.NPM
import com.netflix.spinnaker.keel.api.artifacts.SortingStrategy
import com.netflix.spinnaker.keel.api.schema.Description
import com.netflix.spinnaker.keel.api.schema.SchemaIgnore

/**
 * A [DeliveryArtifact] that describes NPM packages.
 */
data class NpmArtifact(
  @Description("The name of the artifact in the Artifactory. See go/artifactory")
  override val name: String,
  @get:JsonIgnore
  override val deliveryConfigName: String? = null,
  override val reference: String = name,
  override val from: ArtifactOriginFilter = FROM_ANY_BRANCH,
  @SchemaIgnore
  override val isDryRun: Boolean = false,
  @JsonIgnore
  override val metadata: Map<String, Any?> = emptyMap()
) : DeliveryArtifact() {
  override val type = NPM

  override val sortingStrategy: SortingStrategy
    get() = if (filteredBySource) {
      CreatedAtSortingStrategy
    } else {
      NpmVersionSortingStrategy
    }

  override fun withDeliveryConfigName(deliveryConfigName: String) =
    this.copy(deliveryConfigName = deliveryConfigName)

  override fun withDryRunFlag(isDryRun: Boolean) =
    this.copy(isDryRun = isDryRun)

  override fun toString(): String = super.toString()
}
