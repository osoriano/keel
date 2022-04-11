package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id

/**
 * Indicates the reason for the creation of a new environment version.
 */
@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "reason"
)
@JsonSubTypes(
  Type(value = ArtifactChange::class, name = "artifact"),
  Type(value = ResourceChange::class, name = "resource"),
  Type(value = UnknownChange::class, name = "unknown")
)
sealed class EnvironmentChangeReason(
  val reason: String
)

/**
 * A new environment version created because a new artifact version was released.
 */
data class ArtifactChange(
  val artifactName: String,
  val newVersion: String
) : EnvironmentChangeReason("artifact")

/**
 * A new environment version created because the user updated one or more resource definitions.
 */
data class ResourceChange(
  val added: Set<String>,
  val removed: Set<String>,
  val changed: Set<String>
) : EnvironmentChangeReason("resource") {
  constructor(
    currentVersionResourceIdsToVersions: Map<String, Int>,
    newVersionResourceIdsToVersions: Map<String, Int>
  ) : this(
    added = newVersionResourceIdsToVersions.keys - currentVersionResourceIdsToVersions.keys,
    removed = currentVersionResourceIdsToVersions.keys - newVersionResourceIdsToVersions.keys,
    changed = newVersionResourceIdsToVersions.filter { (id, version) ->
      currentVersionResourceIdsToVersions.containsKey(id) && currentVersionResourceIdsToVersions[id] != version
    }.keys
  )
}

/**
 * Just used for legacy data before we added the reason column.
 */
object UnknownChange : EnvironmentChangeReason("unknown")
