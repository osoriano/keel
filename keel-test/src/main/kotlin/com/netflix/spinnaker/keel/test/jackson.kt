package com.netflix.spinnaker.keel.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.netflix.spinnaker.keel.api.artifacts.DEBIAN
import com.netflix.spinnaker.keel.api.artifacts.DOCKER
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import com.netflix.spinnaker.keel.serialization.configuredYamlMapper

fun configuredTestObjectMapper(): ObjectMapper = configuredObjectMapper()
  .registerArtifactSubtypes()

fun configuredTestYamlMapper(): YAMLMapper = configuredYamlMapper()
  .registerArtifactSubtypes() as YAMLMapper

private fun ObjectMapper.registerArtifactSubtypes() =
  apply {
    registerSubtypes(
      NamedType(DebianArtifact::class.java, DEBIAN),
      NamedType(DockerArtifact::class.java, DOCKER),
    )
  }
