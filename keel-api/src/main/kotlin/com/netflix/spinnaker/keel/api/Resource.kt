package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonIgnore

/**
 * Internal representation of a resource.
 */
data class Resource<out T : ResourceSpec>(
  val kind: ResourceKind,
  @get:ExcludedFromDiff
  val metadata: Map<String, Any?>,
  val spec: T
) {
  init {
    require(metadata["id"].isValidId()) { "resource id must be a valid id" }
    require(metadata["application"].isValidApplication()) { "resource application must be a valid application" }
  }

  /**
   * The formal resource id. This formed of the resource's API version prefix and kind and the result of
   * [ResourceSpec.generateId].
   */
  @get:JsonIgnore
  val id: String
    get() = metadata.getValue("id").toString()

  /**
   * A more descriptive name than the [id], intended for displaying in the UI. This property is
   * not persisted.
   */
  val displayName: String
    get() = (spec as? Monikered)?.moniker?.toString() ?: id

  @get:JsonIgnore
  @get:ExcludedFromDiff
  val version: Int
    // version is not a mandatory metadata field, so we default to 0 when missing
    get() = metadata["version"] as? Int ?: 0

  @get:JsonIgnore
  val serviceAccount: String
    get() = metadata.getValue("serviceAccount").toString()

  @get:JsonIgnore
  val application: String
    get() = metadata.getValue("application").toString()

  val basedOn: String?
    get() = metadata["basedOn"] as? String

  val name: String
    get() = (spec as? Monikered)?.moniker?.toString() ?: metadata["name"] as? String ?: id

    /**
   * Attempts to find an artifact in the delivery config based on information in this resource's spec.
   */
  fun findAssociatedArtifact(deliveryConfig: DeliveryConfig) =
    when (spec) {
      is ArtifactReferenceProvider ->
        spec.completeArtifactReferenceOrNull()
          ?.let { ref ->
            deliveryConfig.matchingArtifactByReference(ref.artifactReference)
          }
      else -> null
    }

  /**
   * Adds the specified [suffix] to the resource [id] and all properties of the [spec] derived from it.
   */
  fun deepRename(suffix: String): Resource<T> {
    val updatedSpec = spec.deepRename(suffix)
    return copy(
      spec = updatedSpec as T,
      metadata = metadata + mapOf(
        // this is so the resource ID is updated with the new name/ID (which is in the spec)
        "id" to generateId(kind, updatedSpec, metadata),
        "application" to application
      )
    )
  }

  // TODO: this is kinda dirty, but because we add uid to the metadata when persisting we don't really want to consider it in equality checks
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as Resource<*>

    if (id != other.id) return false
    if (kind != other.kind) return false
    if (spec != other.spec) return false

    return true
  }

  override fun hashCode(): Int {
    var result = id.hashCode()
    result = 31 * result + kind.hashCode()
    result = 31 * result + spec.hashCode()
    return result
  }
}

/**
 * Creates a resource id in the correct format.
 */
fun generateId(kind: ResourceKind, spec: ResourceSpec, metadata: Map<String, Any?>) =
  "${kind.group}:${kind.kind}:${spec.generateId(metadata)}"

private fun Any?.isValidId() =
  when (this) {
    is String -> isNotBlank()
    else -> false
  }

private fun Any?.isValidServiceAccount() =
  when (this) {
    is String -> isNotBlank()
    else -> false
  }

private fun Any?.isValidApplication() =
  when (this) {
    is String -> isNotBlank()
    else -> false
  }
