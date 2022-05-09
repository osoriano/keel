package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.plugins.ResourceHandler
import com.netflix.spinnaker.keel.api.plugins.supporting
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.yaml.APPLICATION_YAML_VALUE
import kotlinx.coroutines.runBlocking
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class ResolvedResourceController(
  private val repository: ResourceRepository,
  private val handlers: List<ResourceHandler<*, *>>
) {

  @GetMapping(
    path = ["/resources/{id}/resolved"],
    produces = [APPLICATION_JSON_VALUE, APPLICATION_YAML_VALUE]
  )
  @PreAuthorize(
    """@authorizationSupport.hasApplicationPermission('READ', 'RESOURCE', #id)
    and @authorizationSupport.hasCloudAccountPermission('READ', 'RESOURCE', #id)"""
  )
  fun getResolved(@PathVariable("id") id: String): Any {
    val resource = repository.get(id)
    return handlers
      .supporting(resource.kind)
      .run {
        runBlocking {
          desired(resource as Resource<Nothing>)
        }
      }
  }
}
