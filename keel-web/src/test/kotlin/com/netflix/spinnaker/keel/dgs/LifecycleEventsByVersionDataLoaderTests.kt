package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.lifecycle.LifecycleEventRepository
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventScope
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventStatus
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventType
import com.netflix.spinnaker.keel.lifecycle.LifecycleStep
import com.netflix.spinnaker.keel.test.debianArtifact
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class LifecycleEventsByVersionDataLoaderTests {
  private val lifecycleEventRepository: LifecycleEventRepository = mockk()
  val subject = LifecycleEventsByVersionDataLoader(lifecycleEventRepository)
  val artifact = debianArtifact()
  private val versions = listOf("version1", "version2", "version3")
  private val artifactAndVersions = versions.map { ArtifactAndVersion(artifact, it) }.toMutableSet()
  private val lifecycleSteps = versions.map {
    LifecycleStep(
      scope = LifecycleEventScope.PRE_DEPLOYMENT,
      type = LifecycleEventType.BUILD,
      id = it,
      status = LifecycleEventStatus.SUCCEEDED,
      artifactVersion = it,
      link = null,
      text = ""
    )
  }

  @Test
  fun `loading versions works`() {
    every {
      lifecycleEventRepository.getSteps(artifact, versions)
    } returns lifecycleSteps

    expectThat(subject.loadData(artifactAndVersions)).isEqualTo(
      artifactAndVersions.zip(lifecycleSteps.map { listOf(it.toDgs()) }).toMap().toMutableMap()
    )
  }
}
