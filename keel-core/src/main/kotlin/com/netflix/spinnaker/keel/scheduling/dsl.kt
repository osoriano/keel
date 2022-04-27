package com.netflix.spinnaker.keel.scheduling

import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.persistence.ResourceHeader

val Resource<*>.header: ResourceHeader
  get() = ResourceHeader(
    id,
    kind,
    metadata["uid"]?.toString() ?: throw IllegalStateException("Resource $id loaded without metadata, this is not a valid way to load resources."),
    application
  )
