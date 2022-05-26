/*
 *
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.config.ArtifactConfig
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.constraints.UpdatedConstraintStatus
import com.netflix.spinnaker.keel.core.api.ActuationPlan
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.events.ApplicationEvent
import com.netflix.spinnaker.keel.igor.DeliveryConfigImporter
import com.netflix.spinnaker.keel.pause.ActuationPauser
import com.netflix.spinnaker.keel.persistence.ResourceRepository.Companion.DEFAULT_MAX_EVENTS
import com.netflix.spinnaker.keel.services.ApplicationService
import com.netflix.spinnaker.keel.services.ApplicationService.Companion.DEFAULT_MAX_ARTIFACT_VERSIONS_FOR_DRY_RUN
import com.netflix.spinnaker.keel.yaml.APPLICATION_YAML_VALUE
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping(path = ["/application"])
@EnableConfigurationProperties(ArtifactConfig::class)
class ApplicationController(
  private val actuationPauser: ActuationPauser,
  private val applicationService: ApplicationService,
  private val deliveryConfigImporter: DeliveryConfigImporter
) {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  @GetMapping(
    path = ["/{application}"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #application)
    and @authorizationSupport.hasCloudAccountPermission('READ', 'APPLICATION', #application)"""
  )
  fun get(
    @PathVariable("application") application: String,
    @RequestParam("entities", required = false, defaultValue = "") entities: MutableList<String>,
    @RequestParam("maxArtifactVersions") maxArtifactVersions: Int?
  ): Map<String, Any> {
    return mutableMapOf<String, Any>(
      "applicationPaused" to actuationPauser.applicationIsPaused(application),
      "hasManagedResources" to applicationService.hasManagedResources(application),
    ).also { results ->

      if (entities.contains("resources")) {
        results["resources"] = applicationService.getResourceSummariesFor(application)
      }
    }
  }

  @GetMapping(
    path = ["/{application}/config"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #application)""")
  fun getConfigByApplication(
    @PathVariable("application") application: String
  ): DeliveryConfig = applicationService.getDeliveryConfig(application)

  // TODO: remove this function after updating the frontend
  @GetMapping(
    path = ["/{application}/config/raw"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #application)""")
  fun getRawConfigByApplication(
    @PathVariable("application") application: String
  ): SubmittedDeliveryConfig = deliveryConfigImporter.import(application, addMetadata = false)

  @DeleteMapping(
    path = ["/{application}/config"]
  )
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #application)"""
  )
  fun deleteConfigByApp(@PathVariable("application") application: String) {
    applicationService.deleteConfigByApp(application)
  }

  @PostMapping(
    path = ["/{application}/environment/{environment}/constraint"],
    consumes = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #application)"""
  )
  fun updateConstraintStatus(
    @RequestHeader("X-SPINNAKER-USER") user: String,
    @PathVariable("application") application: String,
    @PathVariable("environment") environment: String,
    @RequestBody status: UpdatedConstraintStatus
  ) {
    applicationService.updateConstraintStatus(user, application, environment, status)
  }

  @PostMapping(
    path = ["/{application}/pause"]
  )
  @PreAuthorize("@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)")
  fun pause(
    @PathVariable("application") application: String,
    @RequestParam(value = "cancelTasks", required = false) cancelTasks: Boolean = false,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ) {
    actuationPauser.pauseApplication(application, user, null, cancelTasks)
  }

  @DeleteMapping(
    path = ["/{application}/pause"]
  )
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('WRITE', 'APPLICATION', #application)
    and @authorizationSupport.hasServiceAccountAccess('APPLICATION', #application)"""
  )
  fun resume(
    @PathVariable("application") application: String,
    @RequestHeader("X-SPINNAKER-USER") user: String
  ) {
    actuationPauser.resumeApplication(application, user)
  }

  @GetMapping(
    path = ["/{application}/events"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize("""@authorizationSupport.hasApplicationPermission('READ', 'APPLICATION', #application)""")
  fun getEvents(
    @PathVariable("application") application: String,
    @RequestParam("limit") limit: Int?
  ): List<ApplicationEvent> = applicationService.getApplicationEventHistory(application, limit ?: DEFAULT_MAX_EVENTS)

  @PostMapping(
    path = ["/{application}/dry-run"],
    consumes = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  // Not authorized on purpose -- anyone can do a dry-run
  fun dryRun(
    @RequestParam("maxArtifactVersions") maxArtifactVersions: Int?,
    @RequestBody submittedDeliveryConfig: SubmittedDeliveryConfig
  ): ActuationPlan = runBlocking {
    applicationService.dryRun(submittedDeliveryConfig, maxArtifactVersions ?: DEFAULT_MAX_ARTIFACT_VERSIONS_FOR_DRY_RUN)
  }
}
