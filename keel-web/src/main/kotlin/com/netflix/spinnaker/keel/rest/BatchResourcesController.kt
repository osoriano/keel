package com.netflix.spinnaker.keel.rest

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.keel.logging.blankMDC
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.NoDeliveryConfigForApplication
import com.netflix.spinnaker.keel.telemetry.recordDurationPercentile
import com.netflix.spinnaker.kork.exceptions.UserException
import com.netflix.spinnaker.security.AuthenticatedRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.Instant
import kotlin.coroutines.CoroutineContext

/**
 * apis that deal with batch operations
 */
@RestController
@RequestMapping(path = ["/batch"])
class BatchResourcesController(
  private val actuationPauser: ActuationPauser,
){
  private val log by lazy { LoggerFactory.getLogger(BatchResourcesController::class.java) }

  /**
   * Batch pause for resources. This is used as part of initiating a regional failover, called by other tools.
   */
  @PostMapping(
    path = ["/pause"]
  )
  fun pause(
    @RequestBody body: PauseClustersBody
  ) {
    log.debug("Batch pausing ${body.clusterNames.size} resources: {}", body)
    actuationPauser.batchPauseClusters(
      user = body.user ?: "unknown",
      clusters = body.clusterNames,
      titusAccount = getTitusAccount(body.environment),
      ec2Account = getEc2Account(body.environment),
      comment = body.reason
    )
  }

  /**
   * Batch resume for resources. This is used as part of ending a regional failover, called by other tools.
   */
  @PostMapping(
    path = ["/resume"]
  )
  fun resume(
    @RequestBody body: PauseClustersBody
  ) {
    log.debug("Batch resuming ${body.clusterNames.size} resources: {}", body)
    actuationPauser.batchResumeClusters(
      user = body.user ?: "unknown",
      clusters = body.clusterNames,
      titusAccount = getTitusAccount(body.environment),
      ec2Account = getEc2Account(body.environment),
      comment = body.reason
    )
  }

  fun getTitusAccount(environment: String) =
    when (environment) {
      "prod" -> "titusprodvpc"
      "test" -> "titustestvpc"
      else -> throw UnsupportedEnvironmentException(environment, "titus")
    }

  fun getEc2Account(environment: String) =
    when (environment) {
      "prod" -> "prod"
      "test" -> "test"
      else -> throw UnsupportedEnvironmentException(environment, "ec2")
    }

}
class UnsupportedEnvironmentException(
  val environment: String,
  val cloudProvider: String
) : UserException("Unsupported environment '$environment' and cloud provider '$cloudProvider', the only supported env for $cloudProvider is 'prod'")

data class PauseClustersBody(
  val user: String?,
  val reason: String,
  val environment: String, // prod = accounts prod and titusprodvpc
  val clusterNames: List<String>
)

