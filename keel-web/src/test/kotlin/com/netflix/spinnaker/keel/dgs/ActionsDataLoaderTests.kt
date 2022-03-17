package com.netflix.spinnaker.keel.dgs

import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.action.ActionState
import com.netflix.spinnaker.keel.api.action.ActionStateFull
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.action.EnvironmentArtifactAndVersion
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.test.deliveryArtifact
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.time.MutableClock
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationEventPublisher
import strikt.api.expectThat
import strikt.assertions.hasSize

class ActionsDataLoaderTests {

  val publisher: ApplicationEventPublisher = mockk()
  val actionRepository: ActionRepository = mockk()
  val artifact1 = DockerArtifact(
    name = "fnord1",
    reference = "fnord1",
    deliveryConfigName = "config",
    branch = "main"
  )
  val artifact2 = artifact1.copy(name = "fnord2", reference = "fnord2")

  private data class DummyVerification(val value: String) : Verification {
    override val type = "dummyVerification"
    override val id = "$type:$value"
  }

  private val v1 = DummyVerification("1")

  val config = deliveryConfig(configName = "config", env = Environment(name = "test", verifyWith = listOf(v1))).copy(
    artifacts = setOf(artifact1, artifact2)
  )

  val subject = ActionsDataLoader(publisher, actionRepository)

  val key1 = EnvironmentArtifactAndVersion(
    environmentName = "test",
    artifactReference = artifact1.reference,
    artifactVersion = "version1",
    actionType = ActionType.VERIFICATION
  )
  val key2 = key1.copy(artifactVersion = "version2")
  val key3 = key1.copy(artifactReference = artifact2.reference)
  val clock = MutableClock()

  val state1 = ActionStateFull(
    state = ActionState(
      status = ConstraintStatus.PASS,
      startedAt = clock.instant(),
      endedAt = null,
    ),
    type = ActionType.VERIFICATION,
    id = v1.id
  )

  @Test
  fun `actions are mapped correctly`() {
    every {
      actionRepository.getStatesForVersions(any(), artifact1.reference, listOf(key1.artifactVersion, key2.artifactVersion))
    } returns mapOf(
      key1 to listOf(state1),
      key2 to listOf(state1),
    )

    every {
      actionRepository.getStatesForVersions(any(), artifact2.reference, listOf(key3.artifactVersion))
    } returns mapOf(
      key3 to listOf(state1),
    )

    expectThat(subject.loadData(config, mutableSetOf(key1, key2, key3))).hasSize(3)
  }
}
