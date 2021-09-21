package com.netflix.spinnaker.keel.api.plugins

import com.netflix.spinnaker.keel.api.ComputeResourceSpec
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.actuation.Job
import com.netflix.spinnaker.keel.api.actuation.Task
import com.netflix.spinnaker.keel.api.actuation.TaskLauncher
import com.netflix.spinnaker.keel.api.support.EventPublisher
import com.netflix.spinnaker.time.MutableClock
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.clearAllMocks
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNotEmpty
import strikt.assertions.isNull
import strikt.assertions.isTrue
import java.time.Clock

abstract class BaseClusterHandlerTests<
  SPEC: ComputeResourceSpec<*>, // spec type
  RESOLVED: Any, // resolved type
  HANDLER : BaseClusterHandler<SPEC, RESOLVED>
  > {

  abstract fun createSpyHandler(
    resolvers: List<Resolver<*>>,
    clock: Clock,
    eventPublisher: EventPublisher,
    taskLauncher: TaskLauncher,
  ): HANDLER
  abstract fun getSingleRegionCluster(): Resource<SPEC>
  abstract fun getRegions(resource: Resource<SPEC>): List<String>
  abstract fun getMultiRegionCluster(): Resource<SPEC>
  abstract fun getDiffInMoreThanEnabled(resource: Resource<SPEC>): ResourceDiff<Map<String, RESOLVED>>
  abstract fun getDiffOnlyInEnabled(resource: Resource<SPEC>): ResourceDiff<Map<String, RESOLVED>>

  abstract fun getMultiRegionStaggeredDeployCluster(): Resource<SPEC>
  abstract fun getDiffInCapacity(resource: Resource<SPEC>): ResourceDiff<Map<String, RESOLVED>>
  abstract fun getDiffInImage(resource: Resource<SPEC>): ResourceDiff<Map<String, RESOLVED>>

  val clock: Clock = MutableClock()
  val eventPublisher: EventPublisher = mockk(relaxUnitFun = true)
  val resolvers: List<Resolver<*>> = emptyList()
  val taskLauncher: TaskLauncher = mockk()

  data class Fixture<SPEC: ComputeResourceSpec<*>, RESOLVED: Any, HANDLER : BaseClusterHandler<SPEC, RESOLVED>>(
    val handler: HANDLER
  )

  val handler by lazy {
    // we create a spy handler so that we can override the results of functions
    // without having to set up every little bit of cloud specific data
    createSpyHandler(
      resolvers = resolvers,
      clock = clock,
      eventPublisher = eventPublisher,
      taskLauncher = taskLauncher,
    )
  }

  @Test
  fun `handler will take action if diff is in more than enabled`() {
    val resource = getSingleRegionCluster()
    val diff = getDiffInMoreThanEnabled(resource)
    val response = runBlocking { handler.willTakeAction(resource, diff) }
    expectThat(response.willAct).isTrue()
  }

  @Test
  fun `handler will take action if enabled diff and all regions are healthy`() {
    every { handler.getUnhealthyRegionsForActiveServerGroup(any()) } returns emptyList()
    val resource = getSingleRegionCluster()
    val diff = getDiffOnlyInEnabled(resource)
    val response = runBlocking { handler.willTakeAction(resource, diff) }
    expectThat(response.willAct).isTrue()
  }

  @Test
  fun `handler will NOT take action if enabled diff and all regions are NOT healthy`() {
    every { handler.getUnhealthyRegionsForActiveServerGroup(any()) } returns getRegions(getSingleRegionCluster())

    val resource = getSingleRegionCluster()
    val diff = getDiffOnlyInEnabled(resource)
    val response = runBlocking { handler.willTakeAction(resource, diff) }
    expectThat(response.willAct).isFalse()
  }

  @Test
  fun `staggered deploy, multi region, image diff`() {
    val slots = mutableListOf<List<Job>>() // done this way so we can capture the stages for multiple requests
    coEvery { taskLauncher.submitJob(any(), any(), any(), capture(slots)) } returns Task("id", "name")

    val resource = getMultiRegionStaggeredDeployCluster()
    runBlocking { handler.upsert(resource, getDiffInImage(resource)) }

    val firstRegionStages = slots[0]
    val secondRegionStages = slots[1]
    expect {
      // first region
      that(firstRegionStages).isNotEmpty().hasSize(2)
      val deployStage1 = firstRegionStages[0]
      that(deployStage1["type"]).isEqualTo("createServerGroup")
      that(deployStage1["refId"]).isEqualTo("1")
      that(deployStage1["requisiteRefIds"]).isNull()
      val waitStage = firstRegionStages[1]
      that(waitStage["type"]).isEqualTo("wait")
      that(waitStage["refId"]).isEqualTo("2")
      that(waitStage["requisiteStageRefIds"] as? List<*>).isEqualTo(listOf("1"))

      //second region
      that(secondRegionStages).isNotEmpty().hasSize(2)
      val dependsOnExecutionStage = secondRegionStages[0]
      that(dependsOnExecutionStage["type"]).isEqualTo("dependsOnExecution")
      that(dependsOnExecutionStage["refId"]).isEqualTo("1")
      that(dependsOnExecutionStage["requisiteRefIds"]).isNull()
      val deployStage2 = secondRegionStages[1]
      that(deployStage2["type"]).isEqualTo("createServerGroup")
      that(deployStage2["refId"]).isEqualTo("2")
      that(deployStage2["requisiteStageRefIds"] as? List<*>).isEqualTo(listOf("1"))
    }
  }

  @Test
  fun `staggered deploy, multi region, capacity diff (no stagger resize stages)`() {
    val slots = mutableListOf<List<Job>>() // done this way so we can capture the stages for multiple requests
    coEvery { taskLauncher.submitJob(any(), any(), any(), capture(slots)) } returns Task("id", "name")

    val resource = getMultiRegionStaggeredDeployCluster()
    runBlocking { handler.upsert(resource, getDiffInCapacity(resource)) }

    val eastStages = slots[0]
    val westStages = slots[1]
    expect {
      that(eastStages).isNotEmpty().hasSize(1)
      val resizeEast = eastStages[0]
      that(resizeEast["type"]).isEqualTo("resizeServerGroup")
      that(resizeEast["refId"]).isEqualTo("1")
      that(resizeEast["requisiteRefIds"]).isNull()
      that(resizeEast["region"]).isEqualTo("east")

      that(westStages).isNotEmpty().hasSize(1)
      val resizeWest = westStages[0]
      that(resizeWest["type"]).isEqualTo("resizeServerGroup")
      that(resizeWest["refId"]).isEqualTo("1")
      that(resizeWest["requisiteRefIds"]).isNull()
      that(resizeWest["region"]).isEqualTo("west")
    }
  }

  @Test
  fun `non staggered deploy, multi region, image diff`() {
    val slots = mutableListOf<List<Job>>() // done this way so we can capture the stages for multiple requests
    coEvery { taskLauncher.submitJob(any(), any(), any(), capture(slots)) } returns Task("id", "name")

    val resource = getMultiRegionCluster()
    runBlocking { handler.upsert(resource, getDiffInImage(resource)) }

    val firstRegionStages = slots[0]
    val secondRegionStages = slots[1]
    expect {
      // first region
      that(firstRegionStages).isNotEmpty().hasSize(1)
      val deployStage1 = firstRegionStages[0]
      that(deployStage1["type"]).isEqualTo("createServerGroup")
      that(deployStage1["refId"]).isEqualTo("1")

      //second region
      that(secondRegionStages).isNotEmpty().hasSize(1)
      val deployStage2 = secondRegionStages[0]
      that(deployStage2["type"]).isEqualTo("createServerGroup")
      that(deployStage2["refId"]).isEqualTo("1")
    }
  }

  @Test
  fun `non staggered deploy, one region, capacity diff`() {
    val slots = mutableListOf<List<Job>>()
    coEvery { taskLauncher.submitJob(any(), any(), any(), capture(slots)) } returns Task("id", "name")

    val resource = getSingleRegionCluster()
    runBlocking { handler.upsert(resource, getDiffInCapacity(resource)) }
    expect {
      that(slots.size).isEqualTo(1)
      val stages = slots[0]
      that(stages.size).isEqualTo(1)
      that(stages.first()["type"]).isEqualTo("resizeServerGroup")
      that(stages.first()["refId"]).isEqualTo("1")
    }
  }

  @Test
  fun `non staggered deploy, one region, image diff`() {
    val slots = mutableListOf<List<Job>>()
    coEvery { taskLauncher.submitJob(any(), any(), any(), capture(slots)) } returns Task("id", "name")

    val resource = getSingleRegionCluster()
    runBlocking { handler.upsert(resource, getDiffInImage(resource)) }
    expect {
      that(slots.size).isEqualTo(1)
      val stages = slots[0]
      that(stages.size).isEqualTo(1)
      that(stages.first()["type"]).isEqualTo("createServerGroup")
      that(stages.first()["refId"]).isEqualTo("1")
    }
  }
}




