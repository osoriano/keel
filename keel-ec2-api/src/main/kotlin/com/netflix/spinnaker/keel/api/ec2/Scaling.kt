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
package com.netflix.spinnaker.keel.api.ec2

import com.netflix.spinnaker.keel.api.ExcludedFromDiff
import com.netflix.spinnaker.keel.api.schema.Description
import org.apache.commons.lang3.builder.EqualsBuilder
import java.time.Duration

val DEFAULT_AUTOSCALE_INSTANCE_WARMUP: Duration = Duration.ofMinutes(5)
val DEFAULT_AUTOSCALE_SCALE_IN_COOLDOWN: Duration = Duration.ofMinutes(5)
val DEFAULT_AUTOSCALE_SCALE_OUT_COOLDOWN: Duration = Duration.ofMinutes(5)

data class Scaling(
  val suspendedProcesses: Set<ScalingProcess> = emptySet(),
  val targetTrackingPolicies: Set<TargetTrackingPolicy> = emptySet(),
  val stepScalingPolicies: Set<StepScalingPolicy> = emptySet()
)

fun Scaling?.hasScalingPolicies(): Boolean =
  this != null && (targetTrackingPolicies.isNotEmpty() || stepScalingPolicies.isNotEmpty())

sealed class ScalingPolicy<T : ScalingPolicy<T>> {
  /**
   * @return `true` if the configuration of `other` is the same as this policy (ignoring identifiers, just "is this
   * policy going to do the same thing", rather than "is this the same exact policy").
   */
  abstract fun hasSameConfigurationAs(other: T): Boolean
}

data class TargetTrackingPolicy(
  @get:ExcludedFromDiff
  val name: String? = null,
  @Description("Applies only to EC2 clusters")
  val warmup: Duration? = DEFAULT_AUTOSCALE_INSTANCE_WARMUP,
  val targetValue: Double,
  val disableScaleIn: Boolean = false,
  val predefinedMetricSpec: PredefinedMetricSpecification? = null,
  val customMetricSpec: CustomizedMetricSpecification? = null,
  @Description("Applies only to Titus clusters")
  val scaleOutCooldown: Duration? = null,
  @Description("Applies only to Titus clusters")
  val scaleInCooldown: Duration? = null
) : ScalingPolicy<TargetTrackingPolicy>() {
  init {
    require(customMetricSpec != null || predefinedMetricSpec != null) {
      "a custom or predefined metric must be defined"
    }

    require(customMetricSpec == null || predefinedMetricSpec == null) {
      "only one of customMetricSpec or predefinedMetricSpec can be defined"
    }
  }

  // Excluding name, so we can remove policies from current asg when modifying
  override fun hasSameConfigurationAs(other: TargetTrackingPolicy): Boolean =
    EqualsBuilder.reflectionEquals(this, other, TargetTrackingPolicy::name.name)
}

data class StepScalingPolicy(
  @get:ExcludedFromDiff
  val name: String? = null,
  val adjustmentType: String,
  val actionsEnabled: Boolean,
  val comparisonOperator: String,
  val dimensions: Set<MetricDimension>? = emptySet(),
  val evaluationPeriods: Int,
  val period: Duration,
  val threshold: Int,
  val metricName: String,
  val namespace: String,
  val statistic: String,
  @Description("Applies only to EC2 clusters")
  val warmup: Duration? = DEFAULT_AUTOSCALE_INSTANCE_WARMUP,
  val metricAggregationType: String = "Average",
  val stepAdjustments: Set<StepAdjustment>
) : ScalingPolicy<StepScalingPolicy>() {
  init {
    require(stepAdjustments.isNotEmpty()) { "at least one stepAdjustment is required" }
    require(dimensions.isNullOrEmpty() || dimensions.none { it.name == "AutoScalingGroupName" }) {
      "autoscale dimensions should not be scoped to a specific ASG name"
    }
    require(period.multipliedBy(evaluationPeriods.toLong()) <= Duration.ofDays(1)) {
      "period * evaluationPeriods must be less than or equal to 1 day"
    }
  }

  // Excluding name, so we can remove policies from current asg when modifying
  override fun hasSameConfigurationAs(other: StepScalingPolicy): Boolean =
    EqualsBuilder.reflectionEquals(this, other, StepScalingPolicy::name.name)
}

/**
 * @return `true` if this set contains any elements with the same configuration as [other].
 */
fun <POLICY : ScalingPolicy<POLICY>> Set<POLICY>.containsAnyWithSameConfigurationAs(other: POLICY) =
  any { it.hasSameConfigurationAs(other) }

/**
 * @return all elements of this set that use the same configuration as any element in [others].
 */
fun <POLICY : ScalingPolicy<POLICY>> Set<POLICY>.intersectingConfigurations(others: Set<POLICY>): Set<POLICY> =
  filterTo(mutableSetOf()) { others.containsAnyWithSameConfigurationAs(it) }

/**
 * @return all elements of this set that are either not present in [others] or _are_ present but with a greater
 * cardinality than in [others].
 */
fun <POLICY : ScalingPolicy<POLICY>> Set<POLICY>.notPresentOrDuplicatedIn(others: Set<POLICY>): Set<POLICY> {
  val result = toMutableSet()
  others.forEach { policy ->
    // only remove the first matching one, so result will retain any duplicates
    result
      .firstOrNull { it.hasSameConfigurationAs(policy) }
      ?.also { result.remove(it) }
  }
  return result
}

data class MetricDimension(
  val name: String,
  val value: String
)

// https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_CustomizedMetricSpecification.html
data class CustomizedMetricSpecification(
  val name: String,
  val namespace: String,
  val statistic: String,
  val unit: String? = null,
  val dimensions: Set<MetricDimension>? = emptySet()
)

// https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_PredefinedMetricSpecification.html
data class PredefinedMetricSpecification(
  val type: String,
  val label: String? = null
)

// https://docs.aws.amazon.com/autoscaling/ec2/APIReference/API_StepAdjustment.html
data class StepAdjustment(
  val lowerBound: Double? = null,
  val upperBound: Double? = null,
  val scalingAdjustment: Int
)
