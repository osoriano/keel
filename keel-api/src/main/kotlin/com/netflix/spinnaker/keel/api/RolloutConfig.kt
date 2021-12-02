package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.netflix.spinnaker.keel.api.schema.Discriminator
import java.time.Duration


/**
 * When managed rollout is enabled, we will deploy with a ManagedRollout stage instead of
 *   the normal deploy stage.
 *
 * todo eb: specify this at the environment level (optionally), like locations. maybe even in the locations block!
 *
 * Example yaml:
 *
 * rolloutWith:
 *  strategy:
 *   type: staggered
 *   postDeployWait: PT30M # default, can omit
 *   order: # todo eb: take from locations order
 *    - region1
 *    - region2 # todo eb: allow multi region here, like `region2, region3`
 *    - region4
 *   overrides: # optional
 *    us-east-1:
 *      postDeployWait: PT1M
 */
data class RolloutConfig(
  val strategy: RolloutStrategy
)

// duplication of com.netflix.buoy.sdk.model.SelectionStrategy
// so that we don't add another dependency into this module
enum class SelectionStrategy(val enumStyleName: String) {
  alphabetical("ALPHABETICAL"),
  `off-streaming-peak`("OFF_STREAMING_PEAK"),
  staggered("STAGGERED")
}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  property = "type",
  include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes(
  JsonSubTypes.Type(value = Alphabetical::class),
  JsonSubTypes.Type(value = OffStreamingPeak::class),
  JsonSubTypes.Type(value = Staggered::class)
)
abstract class RolloutStrategy {
  @Discriminator
  abstract val type: SelectionStrategy
}

class Alphabetical: RolloutStrategy() {
  override val type = SelectionStrategy.alphabetical
}

class OffStreamingPeak: RolloutStrategy(){
  override val type = SelectionStrategy.`off-streaming-peak`
}

data class Staggered(
  val order: List<String>, //required for now
  val postDeployWait: Duration? = Duration.ofMinutes(30),
  val overrides: Map<String, Map<String, Any>> = emptyMap() // can override postDeployWait
): RolloutStrategy() {
  override val type: SelectionStrategy = SelectionStrategy.staggered
}
