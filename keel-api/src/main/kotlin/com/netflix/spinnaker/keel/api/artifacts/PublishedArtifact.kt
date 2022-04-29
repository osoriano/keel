package com.netflix.spinnaker.keel.api.artifacts

import java.time.Instant

/**
 * An immutable data class that represents a published software artifact in the Spinnaker ecosystem.
 *
 * This class mirrors [com.netflix.spinnaker.kork.artifacts.model.Artifact], but without all the Jackson baggage.
 * One notable difference from the kork counterpart is that this class enforces non-nullability of a few
 * key fields without which it doesn't make sense for an artifact to exist in Managed Delivery terms. It also adds
 * a couple of keel-specific fields to store artifact metadata.
 */
data class PublishedArtifact(
  val name: String,
  val type: String,
  val reference: String,
  val version: String,
  // artifacts come with a tilde in place of an underscore in the version, which breaks clouddriver image lookups
  // (e.g. lpollo-local-test-0.0.1~snapshot instead of lpollo-local-test-0.0.1_snapshot)
  val normalizedVersion: String = version.replaceFirst('~', '_'),
  val customKind: Boolean? = null,
  val location: String? = null,
  val artifactAccount: String? = null,
  val provenance: String? = null,
  val uuid: String? = null,
  val metadata: Map<String, Any?> = emptyMap(),
  // keel-specific fields ---
  val gitMetadata: GitMetadata? = null,
  val buildMetadata: BuildMetadata? = null,
  val storedAt: Instant? = null
) {

  // The stuff that matters to keel
  constructor(
    name: String,
    type: String,
    version: String,
    reference: String? = null,
    status: ArtifactStatus? = null,
    createdAt: Instant? = null,
    storedAt: Instant? = null,
    gitMetadata: GitMetadata? = null,
    buildMetadata: BuildMetadata? = null,
    metadata: Map<String, Any?>? = null
  ) : this(
    name = name,
    type = type.lowercase(),
    reference = reference?: name,
    version = version,
    metadata = (metadata ?: emptyMap()) + mapOf(
      "releaseStatus" to status?.name,
      "createdAt" to createdAt
    ) ,
    gitMetadata = gitMetadata,
    buildMetadata = buildMetadata,
    storedAt = storedAt
  )

  constructor(
    name: String,
    type: String,
    version: String,
    reference: String,
    status: ArtifactStatus? = null,
    createdAt: Instant? = null,
    storedAt: Instant? = null,
    gitMetadata: GitMetadata? = null,
    buildMetadata: BuildMetadata? = null,
    statusInEnvironment: String? = null
  ) : this(
    name = name,
    type = type.lowercase(),
    reference = reference,
    version = version,
    metadata = mapOf(
      "releaseStatus" to status?.name,
      "createdAt" to createdAt,
      "statusInEnvironment" to statusInEnvironment
    ),
    gitMetadata = gitMetadata,
    buildMetadata = buildMetadata,
    storedAt = storedAt
  )

  val status: ArtifactStatus? = metadata["releaseStatus"]?.toString()
    ?.let { ArtifactStatus.valueOf(it) }

  val createdAt: Instant?
    get() = (metadata["createdAt"]
    // docker artifact createdAt time is under date field
      ?: metadata["date"])
      ?.let {
        when (it) {
          is Long -> Instant.ofEpochMilli(it) // to accommodate for artifact events from CI integration
          is Instant -> it
          is String -> try {
            Instant.ofEpochMilli(it.toLong())
          } catch (ex: Exception) {
            null
          }
          else -> null
        }
      }

  val branch: String
    get() = gitMetadata?.branch
      ?: metadata["branch"] as? String
      ?: metadata.getBranchFromBuildTrigger()
      ?: "N/A"

  val commitHash: String
    get() = gitMetadata?.commitInfo?.sha
      ?: gitMetadata?.commit
      ?: metadata["commitId"] as? String
      ?: metadata.getCommitFromBuildDetail()
      ?: "N/A"

  val shortCommitHash: String
    get() = commitHash.shortHash

  val prCommitHash: String?
    get() = (metadata["prCommitId"] as? String)?.let { it.ifEmpty { null } }

  val buildNumber: String
    get() = buildMetadata?.number
      ?: metadata["buildNumber"] as? String
      ?: metadata.getBuildIdFromBuildDetail()
      ?: "-1" //can't do it as N/A as we are parsing int in BuildMetadata.copyOrCreate

  fun normalized() = copy(
    type = type.lowercase(),
    // FIXME: it's silly that we're prepending the artifact name for Debian only...
    version = if (type.lowercase() == DEBIAN && !version.startsWith(name)) "$name-$version" else version
  )

}

val String.shortHash: String
  get() = this.take(7)

fun Map<String, Any?>.getBranchFromBuildTrigger(): String? {
  val event = this["triggerEvent"] as? Map<*, *>
  val target =  event?.get("target") as? Map<*, *>
  return target?.get("branchName") as? String
}

fun Map<String, Any?>.getCommitFromBuildDetail(): String? {
  val buildDetails = this["buildDetail"] as? Map<*, *>
  return buildDetails?.get("commitId") as? String
}

fun Map<String, Any?>.getBuildIdFromBuildDetail(): String? {
  val buildDetails = this["buildDetail"] as? Map<*, *>
  return buildDetails?.get("buildId") as? String
}
