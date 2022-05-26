package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonIgnore
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType

/**
 * Implemented by [ResourceSpec] or concrete resource types that (may) contain artifact information, typically compute
 * resources.
 *
 * The fields on this interface are nullable because typically the spec will _not_ have that information available
 * before the corresponding `ResourceHandler` has resolved the resource.
 */
interface ArtifactProvider {
  @get:JsonIgnore
  val artifactName: String?
  @get:JsonIgnore
  val artifactType: ArtifactType?

  fun completeArtifactOrNull() =
    if (artifactName != null && artifactType != null) {
      CompleteArtifact(artifactName!!, artifactType!!)
    } else {
      null
    }
}

/**
 * Implemented by [ResourceSpec] or concrete resource types that (may) contain versioned artifacts, typically compute
 * resources.
 */
interface VersionedArtifactProvider : ArtifactProvider {
  @get:JsonIgnore
  val artifactVersion: String?
}

/**
 * Implemented by [ResourceSpec] or concrete resource types that (may) contain artifact references, typically compute
 * resources.
 */
interface ArtifactReferenceProvider : ResourceSpec {
  val artifactReference: String?

  fun completeArtifactReferenceOrNull() =
    if (artifactReference != null) {
      CompleteArtifactReference(artifactReference!!)
    } else {
      null
    }

  fun withArtifactReference(reference: String): ArtifactReferenceProvider
}

/**
 * Simple container of the information defined in [ArtifactProvider] that ensures non-nullability of the fields.
 */
data class CompleteArtifact(
  override val artifactName: String,
  override val artifactType: ArtifactType
) : ArtifactProvider

/**
 * Simple container of the information defined by [VersionedArtifactProvider] that ensures non-nullability of the
 * fields.
 */
data class CompleteVersionedArtifact(
  override val artifactName: String,
  override val artifactType: ArtifactType,
  override val artifactVersion: String
) : VersionedArtifactProvider

/**
 * Simple container of the information defined by [ArtifactReferenceProvider] that ensures non-nullability of the
 * fields.
 */
data class CompleteArtifactReference(
  val artifactReference: String
)
