package com.netflix.spinnaker.keel.actuation

import com.netflix.spectator.api.NoopRegistry
import com.netflix.spinnaker.config.ArtifactCheckConfig
import com.netflix.spinnaker.config.DefaultWorkhorseCoroutineContext
import com.netflix.spinnaker.config.EnvironmentCheckConfig
import com.netflix.spinnaker.config.EnvironmentDeletionConfig
import com.netflix.spinnaker.config.EnvironmentVerificationConfig
import com.netflix.spinnaker.config.PostDeployActionsConfig
import com.netflix.spinnaker.config.ResourceCheckConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.artifacts.VirtualMachineOptions
import com.netflix.spinnaker.keel.api.artifacts.fromBranch
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.persistence.AgentLockRepository
import com.netflix.spinnaker.keel.persistence.EnvironmentDeletionRepository
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.postdeploy.PostDeployActionRunner
import com.netflix.spinnaker.keel.scheduled.ScheduledAgent
import com.netflix.spinnaker.keel.telemetry.EnvironmentCheckStarted
import com.netflix.spinnaker.keel.test.deliveryConfig
import com.netflix.spinnaker.keel.verification.VerificationRunner
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.clearAllMocks
import io.mockk.coVerify as verify
import io.mockk.coEvery as every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import org.springframework.context.ApplicationEventPublisher
import org.springframework.core.env.Environment as SpringEnvironment
import java.time.Duration

internal class CheckSchedulerTests : JUnit5Minutests {

  private val repository: KeelRepository = mockk()
  private val postDeployActionRunner: PostDeployActionRunner = mockk()
  private val environmentPromotionChecker = mockk<EnvironmentPromotionChecker>()
  private val artifactHandler = mockk<ArtifactHandler>(relaxUnitFun = true)
  private val publisher = mockk<ApplicationEventPublisher>(relaxUnitFun = true)
  private val registry = NoopRegistry()
  private val checkMinAge = Duration.ofMinutes(5)
  private val resourceCheckConfig = ResourceCheckConfig().also {
    it.minAgeDuration = checkMinAge
    it.batchSize = 2
  }
  private val artifactCheckConfig = ArtifactCheckConfig().also {
    it.minAgeDuration = checkMinAge
    it.batchSize = 2
  }
  private val environmentCheckConfig = EnvironmentCheckConfig().also {
    it.minAgeDuration = checkMinAge
    it.batchSize = 2
  }
  private val verificationConfig = EnvironmentVerificationConfig().also {
    it.minAgeDuration = checkMinAge
    it.batchSize = 2
    it.timeoutDuration = Duration.ofMinutes(2)
  }
  private val postDeployConfig = PostDeployActionsConfig().also {
    it.minAgeDuration = checkMinAge
    it.batchSize = 2
  }

  private val springEnv: SpringEnvironment = mockk(relaxed = true) {
    every {
      getProperty("keel.check.min-age-duration", Duration::class.java, any())
    } returns checkMinAge

    every { getProperty("keel.resource-check.batch-size", Int::class.java, any()) } returns resourceCheckConfig.batchSize
    every { getProperty("keel.environment-check.batch-size", Int::class.java, any()) } returns environmentCheckConfig.batchSize
    every { getProperty("keel.artifact-check.batch-size", Int::class.java, any()) } returns artifactCheckConfig.batchSize
    every { getProperty("keel.resource.wait-for-batch.enabled", Boolean::class.java, any()) } returns false
    every { getProperty("keel.environment.wait-for-batch.enabled", Boolean::class.java, any()) } returns false
    every { getProperty("keel.environment-deletion.wait-for-batch.enabled", Boolean::class.java, any()) } returns false
    every { getProperty("keel.artifact.wait-for-batch.enabled", Boolean::class.java, any()) } returns false
    every { getProperty("keel.verification.wait-for-batch.enabled", Boolean::class.java, any()) } returns false
    every { getProperty("keel.post-deploy.wait-for-batch.enabled", Boolean::class.java, any()) } returns false
  }


  class DummyScheduledAgent(override val lockTimeoutSeconds: Long) : ScheduledAgent {
    override suspend fun invokeAgent() {
    }
  }

  private val dummyAgent = mockk<DummyScheduledAgent>(relaxUnitFun = true) {
    every {
      lockTimeoutSeconds
    } returns 5
  }

  private var agentLockRepository = mockk<AgentLockRepository>(relaxUnitFun = true) {
    every { agents } returns listOf(dummyAgent)
  }

  private val verificationRunner = mockk<VerificationRunner>()

  private val environmentDeletionRepository: EnvironmentDeletionRepository = mockk()

  private val environmentCleaner: EnvironmentCleaner = mockk()

  private val artifacts = listOf(
    DebianArtifact(
      name = "fnord",
      vmOptions = VirtualMachineOptions(
        baseOs = "bionic-classic",
        regions = setOf("us-west-2", "us-east-1")
      ),
      from = fromBranch("main")
    ),
    DockerArtifact(
      name = "fnord-but-like-in-a-container",
      branch = "main"
    )
  )

  private val deliveryConfigs = listOf(
    deliveryConfig(application = "app1", configName = "app1"),
    deliveryConfig(application = "app2", configName = "app2")
  )

  private val environmentsForDeletion = listOf(
    Environment("my-preview-environment1"),
    Environment("my-preview-environment2")
  )

  fun tests() = rootContext<CheckScheduler> {
    fixture {
      CheckScheduler(
        repository = repository,
        environmentDeletionRepository = environmentDeletionRepository,
        environmentPromotionChecker = environmentPromotionChecker,
        postDeployActionRunner = postDeployActionRunner,
        artifactHandlers = listOf(artifactHandler),
        resourceCheckConfig = resourceCheckConfig,
        environmentCheckConfig = environmentCheckConfig,
        artifactCheckConfig = artifactCheckConfig,
        verificationConfig = verificationConfig,
        postDeployConfig = postDeployConfig,
        environmentDeletionConfig = EnvironmentDeletionConfig(),
        environmentCleaner = environmentCleaner,
        publisher = publisher,
        agentLockRepository = agentLockRepository,
        verificationRunner = verificationRunner,
        clock = MutableClock(),
        springEnv = springEnv,
        spectator = registry,
        coroutineContext = DefaultWorkhorseCoroutineContext
      )
    }

    context("scheduler is enabled") {
      before {
        onApplicationUp()
      }

      after {
        onApplicationDown()
      }

      context("checking environments") {
        before {
          every {
            repository.deliveryConfigsDueForCheck(any(), any())
          } returns deliveryConfigs

          every {
            repository.markDeliveryConfigCheckComplete(any())
          } just runs

          every {
            environmentPromotionChecker.checkEnvironments(any())
          } just runs

          checkEnvironments()
        }

        test("all delivery configs due are checked") {
          deliveryConfigs.forEach {
            verify {
              environmentPromotionChecker.checkEnvironments(it)
            }
          }
        }

        test("a telemetry event is published for each delivery config check") {
          deliveryConfigs.forEach {
            verify {
              publisher.publishEvent(EnvironmentCheckStarted(it))
            }
          }
        }
      }

      context("checking artifacts") {
        before {
          every {
            repository.artifactsDueForCheck(any(), any())
          } returns artifacts

          checkArtifacts()
        }

        test("all artifacts are checked") {
          artifacts.forEach { artifact ->
            verify(timeout = 500) {
              artifactHandler.handle(artifact)
            }
          }
        }
      }

      context("checking environments for deletion") {
        before {
          every {
            environmentDeletionRepository.itemsDueForCheck(any(), any())
          } returns environmentsForDeletion

          every {
            environmentCleaner.cleanupEnvironment(any())
          } just runs

          checkEnvironmentsForDeletion()
        }

        test("all environments marked for deletion are checked for cleanup") {
          environmentsForDeletion.forEach { environment ->
            verify(timeout = 500) {
              environmentCleaner.cleanupEnvironment(environment)
            }
          }
        }
      }
    }

    context("test invoke agents") {
      before {
        onApplicationUp()
      }

      test("invoke a single agent") {
        every {
          agentLockRepository.tryAcquireLock(any(), any())
        } returns true

        invokeAgent()

        verify {
          dummyAgent.invokeAgent()
        }
      }
      after {
        onApplicationDown()
        clearAllMocks()
      }
    }
  }
}
