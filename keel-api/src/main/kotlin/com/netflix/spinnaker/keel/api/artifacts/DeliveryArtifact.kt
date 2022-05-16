package com.netflix.spinnaker.keel.api.artifacts

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access.WRITE_ONLY
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.ArtifactReferenceProvider
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.ExcludedFromDiff
import com.netflix.spinnaker.keel.api.schema.Description
import com.netflix.spinnaker.keel.api.schema.Discriminator
import com.netflix.spinnaker.keel.api.schema.SchemaIgnore
import com.netflix.spinnaker.keel.api.schema.Title
import java.time.Instant

typealias ArtifactType = String

const val DEBIAN: ArtifactType = "deb"
const val DOCKER: ArtifactType = "docker"
const val NPM: ArtifactType = "npm"

/**
 * The release status of an artifact. This may not necessarily be applicable to all
 * [DeliveryArtifact] sub-classes.
 */
enum class ArtifactStatus {
  FINAL, CANDIDATE, SNAPSHOT, RELEASE, UNKNOWN
}

/**
 * Filters for source code branches. The fields within this class are mutually-exclusive.
 *
 * @param name A specific branch to match against.
 * @param startsWith Match branches starting with this string.
 * @param regex A regular expression to match against (e.g. "feature-.*").
 */
data class BranchFilter(
  val name: String? = null,
  val startsWith: String? = null,
  val regex: String? = null
) {
  init {
    require(
      (name != null && startsWith == null && regex == null)
        || (name == null && startsWith != null && regex == null)
        || (name == null && startsWith == null && regex != null)
    ) {
      "Please specify only one of 'name', 'startsWith' or 'regex'."
    }
  }

  fun matches(branch: String): Boolean =
    when {
      name != null -> branch == name
      startsWith != null -> branch.startsWith(startsWith)
      regex != null -> Regex(regex).matches(branch)
      else -> false
    }

  override fun toString(): String =
    when {
      name != null -> "Branch name is '$name'"
      startsWith != null -> "Branch name starts with '$startsWith"
      regex != null -> "Branch name matches regex '$regex'"
      else -> "Malformed branch filter. This is a bug."
    }
}

// Utility functions to create branch filters
fun branchStartsWith(startsWith: String) = BranchFilter(startsWith = startsWith)
fun branchName(name: String) = BranchFilter(name = name)
fun branchRegex(regex: String) = BranchFilter(regex = regex)
fun from(branchFilter: BranchFilter) = ArtifactOriginFilter(branch = branchFilter)
fun fromBranch(branch: String) = from(BranchFilter(name = branch))

// Short handle for a branch filter that matches any branch
val FROM_ANY_BRANCH = from(branchRegex(".*"))

/**
 * Filters for the origin of an artifact in source control.
 *
 * @param branch A [BranchFilter] with branch filters.
 * @param pullRequestOnly Whether to include only artifacts built from pull requests.
 */
@Title("Source")
data class ArtifactOriginFilter(
  val branch: BranchFilter? = null,
  @Description("Whether to include only artifacts built from pull requests")
  val pullRequestOnly: Boolean? = false
)

/**
 * An artifact as defined in a [com.netflix.spinnaker.keel.api.DeliveryConfig].
 *
 * Unlike other places within Spinnaker, this class does not describe a specific instance of a software artifact
 * (i.e. the output of a build that is published to an artifact repository), but rather the high-level properties
 * that allow keel and [com.netflix.spinnaker.keel.api.plugins.ArtifactSupplier] plugins to find/process the actual
 * artifacts.
 */
// TODO: rename to `ArtifactSpec` or `ArtifactConfig`
@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type"
)
abstract class DeliveryArtifact {
  abstract val name: String
  @Discriminator
  abstract val type: ArtifactType
  @get:JsonIgnore
  abstract val sortingStrategy: SortingStrategy

  /** A friendly reference to use within a delivery config. */
  abstract val reference: String

  /** The delivery config this artifact is a part of. */
  @get:JsonProperty(access = WRITE_ONLY)
  @get:ExcludedFromDiff
  abstract val deliveryConfigName: String?

  abstract fun withDeliveryConfigName(deliveryConfigName: String): DeliveryArtifact

  /** Filters for the artifact origin in source control. */
  open val from: ArtifactOriginFilter? = null

  /** Whether this artifact was created for a preview environment. */
  @get:JsonProperty(access = WRITE_ONLY)
  open val isPreview: Boolean = false

  /** Arbitrary metadata that can be used to augment the generic configuration of the artifact */
  @get:JsonInclude(NON_EMPTY)
  open val metadata: Map<String, Any?> = emptyMap()

  @get:JsonIgnore
  @get:ExcludedFromDiff
  val filteredByBranch: Boolean
    get() = from?.branch != null

  @get:JsonIgnore
  @get:ExcludedFromDiff
  val filteredByPullRequest: Boolean
    get() = from?.pullRequestOnly == true

  @get:JsonIgnore
  @get:ExcludedFromDiff
  val filteredBySource: Boolean
    get() = filteredByBranch || filteredByPullRequest

  fun hasMatchingSource(gitMetadata: GitMetadata?): Boolean {
    return when {
      this.filteredBySource && gitMetadata == null -> false
      this.filteredByBranch && gitMetadata != null -> if (gitMetadata.branch == null) {
        false
      } else {
        this.from?.branch?.matches(gitMetadata.branch) ?: true
      }
      this.filteredByPullRequest && gitMetadata != null -> gitMetadata.pullRequest != null
      else -> true
    }
  }

  /**
   * Given the additional details about a version and git/build metadata, return a [PublishedArtifact] representing
   * that version for this [DeliveryArtifact].
   */
  fun toArtifactVersion(
    version: String,
    status: ArtifactStatus? = null,
    createdAt: Instant? = null,
    storedAt: Instant? = null,
    gitMetadata: GitMetadata? = null,
    buildMetadata: BuildMetadata? = null,
    metadata: Map<String, Any?> = emptyMap()
  ) =
    PublishedArtifact(
      name = name,
      type = type,
      reference = reference,
      version = version,
      metadata = metadata + mapOf(
        "releaseStatus" to status,
        "createdAt" to createdAt
      ),
      gitMetadata = gitMetadata,
      buildMetadata = buildMetadata,
      storedAt = storedAt
    ).normalized()

  /**
   * @return `true` if this artifact is used by any resource in [environment], `false` otherwise.
   */
  fun isUsedIn(environment: Environment) =
    environment
      .resources
      .map { (it.spec as? ArtifactReferenceProvider)?.artifactReference }
      .contains(reference)

  /**
   * returns the resource ids using the artifact in the environment
   */
  fun resourcesUsing(environment: Environment) =
    environment
      .resources
      .filter { reference == (it.spec as? ArtifactReferenceProvider)?.artifactReference }
      .map { it.id }

  override fun toString() = "${type.uppercase()} artifact $name (ref: $reference)"
}
