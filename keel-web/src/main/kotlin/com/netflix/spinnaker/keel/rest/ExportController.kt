package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import com.netflix.spinnaker.keel.export.ExportErrorResult
import com.netflix.spinnaker.keel.export.ExportResult
import com.netflix.spinnaker.keel.export.ExportService
import com.netflix.spinnaker.keel.retrofit.isNotFound
import com.netflix.spinnaker.keel.yaml.APPLICATION_YAML_VALUE
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR
import org.springframework.http.HttpStatus.NOT_FOUND
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping(path = ["/export"])
class ExportController(
  private val exportService: ExportService
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  companion object {
    const val DEFAULT_PIPELINE_EXPORT_MAX_DAYS: Long = 180
  }

  /**
   * This route is location-less; given a resource name that can be monikered,
   * type, and account, all locations configured for the account are scanned for
   * matching resources that can be combined into a multi-region spec.
   *
   * Types are derived from Clouddriver's naming convention. It is assumed that
   * converting these to kebab case (i.e. securityGroups -> security-groups) will
   * match either the singular or plural of a [ResourceHandler]'s
   * [ResourceHandler.supportedKind].
   */
  @GetMapping(
    path = ["/{cloudProvider}/{account}/{type}/{name}"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun get(
    @PathVariable("cloudProvider") cloudProvider: String,
    @PathVariable("account") account: String,
    @PathVariable("type") type: String,
    @PathVariable("name") name: String,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): SubmittedResource<*> {
    return runBlocking {
      exportService.exportResource(cloudProvider, type, account, name, user)
    }
  }

  @GetMapping(
    path = ["/artifact/{cloudProvider}/{account}/{clusterName}"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  fun getArtifactFromCluster(
    @PathVariable("cloudProvider") cloudProvider: String,
    @PathVariable("account") account: String,
    @PathVariable("clusterName") name: String,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ): DeliveryArtifact? {
    return runBlocking {
      exportService.exportArtifact(cloudProvider, account, name, user)
    }
  }

  @GetMapping(
    path = ["/{application}"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #application)""")
  fun get(
    @PathVariable("application") application: String,
    @RequestParam("maxAgeDays") maxAgeDays: Long?
  ): ExportResult {
    return runBlocking {
      exportService.exportFromPipelines(application, maxAgeDays ?: DEFAULT_PIPELINE_EXPORT_MAX_DAYS)
    }
  }

  @ExceptionHandler(Exception::class)
  fun onException(e: Exception): ResponseEntity<ExportErrorResult> {
    log.error(e.message, e)
    return when (e.cause?.isNotFound ?: e.isNotFound) {
      true -> ResponseEntity.status(NOT_FOUND).body(ExportErrorResult(e.message ?: "not found"))
      else -> ResponseEntity.status(INTERNAL_SERVER_ERROR).body(ExportErrorResult(e.message ?: "unknown error"))
    }
  }
}
