package com.netflix.spinnaker.keel.test

import com.netflix.spinnaker.keel.api.artifacts.ArtifactOriginFilter
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.api.artifacts.SortingStrategy
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.artifacts.branchName
import com.netflix.spinnaker.keel.api.artifacts.fromBranch
import com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier
import com.netflix.spinnaker.keel.api.support.SpringEventPublisherBridge
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DebianArtifactSupplier
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifactSupplier
import com.netflix.spinnaker.keel.artifacts.NpmArtifactSupplier
import com.netflix.spinnaker.keel.igor.artifact.ArtifactMetadataService
import com.netflix.spinnaker.keel.igor.artifact.ArtifactService
import com.netflix.spinnaker.keel.titus.registry.TitusRegistryService
import io.mockk.mockk
import org.springframework.core.env.Environment

data class DummyArtifact(
  override val name: String = "fnord",
  override val deliveryConfigName: String? = "manifest",
  override val reference: String = "fnord",
  override val isDryRun: Boolean = false
) : DeliveryArtifact() {
  override val type: ArtifactType = "dummy"
  override val sortingStrategy = DummySortingStrategy
  override fun withDeliveryConfigName(deliveryConfigName: String) =
    this.copy(deliveryConfigName = deliveryConfigName)

  override fun withDryRunFlag(isDryRun: Boolean) =
    this.copy(isDryRun = isDryRun)
}

object DummySortingStrategy : SortingStrategy {
  override val comparator: Comparator<PublishedArtifact> = compareByDescending { it.version }
  override val type = "dummy"
}

fun defaultArtifactSuppliers(): List<ArtifactSupplier<*, *>> {
  val artifactService: ArtifactService = mockk(relaxUnitFun = true)
  val eventBridge: SpringEventPublisherBridge = mockk(relaxUnitFun = true)
  val artifactMetadataService: ArtifactMetadataService = mockk(relaxUnitFun = true)
  val springEnv: Environment = mockk(relaxed = true)
  val titusRegistryService: TitusRegistryService = mockk()
  return listOf(
    DebianArtifactSupplier(eventBridge, artifactService, artifactMetadataService),
    DockerArtifactSupplier(eventBridge, artifactMetadataService, titusRegistryService),
    NpmArtifactSupplier(eventBridge, artifactService, artifactMetadataService)
  )
}

fun debianArtifact(name: String = "fnord", reference: String = "$name-deb", metadata: Map<String, Any?> = emptyMap()) = DebianArtifact(
  name = name,
  reference = reference,
  deliveryConfigName = "fnord-manifest",
  vmOptions = VirtualMachineOptions(baseOs = "bionic", regions = setOf("us-west-2")),
  from = fromBranch("main"),
  metadata = metadata
)

fun dockerArtifact(metadata: Map<String, Any?> = emptyMap()) = DockerArtifact(
  name = "org/fnord",
  reference = "fnord-docker",
  deliveryConfigName = "fnord-manifest",
  from = ArtifactOriginFilter(branchName("main")),
  metadata = metadata
)
