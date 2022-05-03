package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.schema.SchemaIgnore
import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.BranchFilter
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.FROM_ANY_BRANCH
import com.netflix.spinnaker.keel.api.artifacts.SortingStrategy
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.artifacts.branchRegex

/**
 * A [DeliveryArtifact] that describes Debian packages.
 */
data class DebianArtifact(
  override val name: String,
  override val deliveryConfigName: String? = null,
  override val reference: String = name,
  val vmOptions: VirtualMachineOptions,
  override val from: ArtifactOriginFilter = FROM_ANY_BRANCH,
  @SchemaIgnore
  override val metadata: Map<String, Any?> = emptyMap()
) : DeliveryArtifact() {
  override val type = DEBIAN

  override val sortingStrategy: SortingStrategy
    get() = if (filteredBySource) {
      CreatedAtSortingStrategy
    } else {
      DebianVersionSortingStrategy
    }

  override fun withDeliveryConfigName(deliveryConfigName: String): DeliveryArtifact {
    return this.copy(deliveryConfigName = deliveryConfigName)
  }

  override fun toString(): String = super.toString()
}
