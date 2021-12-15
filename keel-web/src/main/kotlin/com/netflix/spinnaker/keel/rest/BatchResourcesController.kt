package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.kork.exceptions.UserException
import com.netflix.spinnaker.security.AuthenticatedRequest
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * apis that deal with batch operations
 */
@RestController
@RequestMapping(path = ["/batch"])
class BatchResourcesController(
  private val actuationPauser: ActuationPauser,
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * Batch pause for resources. This is used as part of initiating a regional failover, called by other tools.
   */
  @PostMapping(
    path = ["/pause"]
  )
  fun pause(
    @RequestBody body: PauseClustersBody
  ) {
    log.debug("Batch pausing resources: {}", body)
    body.clusterNames.forEach { clusterName ->
        actuationPauser.pauseCluster(
          name = clusterName,
          titusAccount = getTitusAccount(body.environment),
          ec2Account = getEc2Account(body.environment),
          user = AuthenticatedRequest.getSpinnakerUser().orElse(body.user ?: "unknown"),
          comment = body.reason
        )
      }
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
    log.debug("Batch resuming resources: {}", body)
    body.clusterNames.forEach { clusterName ->
      actuationPauser.resumeCluster(
        name = clusterName,
        titusAccount = getTitusAccount(body.environment),
        ec2Account = getEc2Account(body.environment),
        user = AuthenticatedRequest.getSpinnakerUser().orElse(body.user ?: "unknown"),
        comment = body.reason
      )
    }
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

data class PauseClustersBody(
  val user: String?,
  val reason: String,
  val environment: String, // prod = accounts prod and titusprodvpc
  val clusterNames: List<String>
)

class UnsupportedEnvironmentException(
  val environment: String,
  val cloudProvider: String
) : UserException("Unsupported environment '$environment' and cloud provider '$cloudProvider', the only supported env for $cloudProvider is 'prod'")

