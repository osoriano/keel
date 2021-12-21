package com.netflix.spinnaker.keel.diff

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.spinnaker.keel.api.ExcludedFromDiff
import com.netflix.spinnaker.keel.api.ResourceDiff
import com.netflix.spinnaker.keel.api.ResourceDiffFactory
import com.netflix.spinnaker.keel.api.ec2.Scaling
import com.netflix.spinnaker.keel.api.ec2.ScalingPolicy
import com.netflix.spinnaker.keel.api.ec2.StepScalingPolicy
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import de.danielbechler.diff.ObjectDiffer
import de.danielbechler.diff.ObjectDifferBuilder
import de.danielbechler.diff.comparison.ComparisonStrategy
import de.danielbechler.diff.comparison.EqualsOnlyComparisonStrategy
import de.danielbechler.diff.inclusion.Inclusion
import de.danielbechler.diff.inclusion.InclusionResolver
import de.danielbechler.diff.node.DiffNode
import java.time.Duration
import java.time.Instant

/**
 * You shouldn't use this class directly. Create instances using [DefaultResourceDiffFactory.compare].
 */
class DefaultResourceDiff<T : Any> internal constructor(
  private val differ: ObjectDiffer,
  override val desired: T,
  override val current: T?
) : ResourceDiff<T> {
  val diff: DiffNode by lazy {
    differ.compare(
      desired,
      current
    )
  }

  override fun hasChanges(): Boolean = diff.hasChanges()

  override val affectedRootPropertyTypes: List<Class<*>>
    get() = children.map { it.valueType }.toList()

  override val affectedRootPropertyNames: Set<String>
    get() = children.map { it.propertyName }.toSet()

  val children: Set<DiffNode>
    get() = mutableSetOf<DiffNode>()
      .also { nodes ->
        diff.visitChildren { node, visit ->
          visit.dontGoDeeper()
          nodes += node
        }
      }

  override fun toDeltaJson(): Map<String, Any?> =
    JsonVisitor(desired, current, "desired", "current")
      .also { diff.visit(it) }
      .messages

  override fun toUpdateJson(): Map<String, Any?> =
    JsonVisitor(desired, current, "updated", "previous")
      .also { diff.visit(it) }
      .messages

  override fun toDebug(): String =
    DebugVisitor(desired, current)
      .also { diff.visit(it) }
      .toString()

  override fun T?.toMap(): Map<String, Any?>? =
    when (this) {
      null -> null
      else -> mapper.convertValue(this)
    }

  companion object {
    val mapper: ObjectMapper = configuredObjectMapper()
  }
}

fun <T : Any> ResourceDiffFactory.toIndividualDiffs(diff: ResourceDiff<Map<String, T>>): List<ResourceDiff<T>> =
  diff.desired.map { (key, value) ->
    compare(value, diff.current?.get(key))
  }

/**
 * Factory for producing [DefaultResourceDiff] instances by comparing two objects.
 */
class DefaultResourceDiffFactory(
  identityServiceCustomizers: Iterable<IdentityServiceCustomizer> = emptyList()
) : ResourceDiffFactory {
  /**
   * Because [Scaling] contains sets of [ScalingPolicy] types that have properties that should be ignored for the
   * purposes of diffing we need to customize the way the differ treats this type. By default, it has to rely on
   * `equals` / `hashCode` to compare objects in sets, but because of the ignored `name` property on these types that
   * won't work. If we customize the `equals` and `hashCode` to also ignore the field it means we're incapable of
   * detecting the presence of duplicate policies with different names. This strategy converts the [Scaling] object to
   * JSON and compares that.
   */
  private object ScalingComparisonStrategy : ComparisonStrategy {
    interface ScalingPolicyMixin {
      @get:JsonIgnore
      val name: String?
    }

    private val mapper = configuredObjectMapper()
      .addMixIn(ScalingPolicy::class.java, ScalingPolicyMixin::class.java)

    override fun compare(node: DiffNode, type: Class<*>, working: Any?, base: Any?) {
      val w = (working as? Scaling?).withConsistentOrdering()
      val b = (base as? Scaling?).withConsistentOrdering()
      EqualsOnlyComparisonStrategy()
        .compare(node, type, mapper.writeValueAsString(w), mapper.writeValueAsString(b))
    }

    private val TargetTrackingPolicy.metricName: String
      get() = checkNotNull(predefinedMetricSpec?.type ?: customMetricSpec?.let { "${it.namespace}/${it.name}" })

    private val targetTrackingPolicyComparator = Comparator<TargetTrackingPolicy> { o1, o2 ->
      o1.metricName.compareTo(o2.metricName)
    }
    private val stepScalingPolicyComparator = Comparator<StepScalingPolicy> { o1, o2 ->
      "${o1.namespace}/${o1.metricName}:${o1.threshold}".compareTo("${o2.namespace}/${o2.metricName}:${o2.threshold}")
    }

    private fun Scaling?.withConsistentOrdering() =
      this?.copy(
        targetTrackingPolicies = targetTrackingPolicies.sortedWith(targetTrackingPolicyComparator).toSet(),
        stepScalingPolicies = stepScalingPolicies.sortedWith(stepScalingPolicyComparator).toSet()
      )
  }

  private val objectDiffer = ObjectDifferBuilder
    .startBuilding()
    .apply {
      comparison()
        .apply {
          ofType<Instant>().toUseEqualsMethod()
          ofType<Duration>().toUseEqualsMethod()
          ofType<Scaling>().toUse(ScalingComparisonStrategy)
        }
      inclusion()
        .resolveUsing(object : InclusionResolver {
          override fun getInclusion(node: DiffNode): Inclusion =
            if (node.getPropertyAnnotation<ExcludedFromDiff>() != null) Inclusion.EXCLUDED else Inclusion.INCLUDED

          override fun enablesStrictIncludeMode() = false
        })
      identity()
        .apply {
          identityServiceCustomizers.forEach { it.customize(this) }
        }
      differs()
        .register(PolymorphismAwareDifferFactory(this))
    }
    .build()

  override fun <T : Any> compare(desired: T, current: T?) =
    DefaultResourceDiff(objectDiffer, desired, current)
}
