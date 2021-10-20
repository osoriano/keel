package com.netflix.spinnaker.keel.diff

import de.danielbechler.diff.NodeQueryService
import de.danielbechler.diff.ObjectDifferBuilder
import de.danielbechler.diff.access.Instances
import de.danielbechler.diff.comparison.ComparisonService
import de.danielbechler.diff.differ.BeanDiffer
import de.danielbechler.diff.differ.Differ
import de.danielbechler.diff.differ.DifferDispatcher
import de.danielbechler.diff.differ.DifferFactory
import de.danielbechler.diff.filtering.ReturnableNodeService
import de.danielbechler.diff.introspection.IntrospectionService
import de.danielbechler.diff.node.DiffNode
import java.lang.reflect.Modifier

class PolymorphismAwareDifferFactory(
  private val objectDifferBuilder: ObjectDifferBuilder
) : DifferFactory {
  override fun createDiffer(
    differDispatcher: DifferDispatcher,
    nodeQueryService: NodeQueryService
  ): Differ = object : Differ {
    // ugh, wish I could just get the already built one, but there's no hook to access it
    private val delegate = BeanDiffer(
      differDispatcher,
      IntrospectionService(objectDifferBuilder),
      ReturnableNodeService(objectDifferBuilder),
      ComparisonService(objectDifferBuilder),
      IntrospectionService(objectDifferBuilder)
    )

    override fun accepts(type: Class<*>): Boolean =
      // we don't want to handle collections as the existing differ works
      !type.isFinal && !type.isCollection && !type.isMap

    override fun compare(parentNode: DiffNode?, instances: Instances): DiffNode =
      if (instances.areDifferentSubTypes()) {
        DiffNode(parentNode, instances.sourceAccessor, instances.type).apply { state = DiffNode.State.CHANGED }
      } else {
        delegate.compare(parentNode, instances)
      }

    private fun Instances.areDifferentSubTypes(): Boolean =
      if (working == null || base == null) {
        // will get resolved as ADDED or REMOVED by regular differ
        false
      } else {
        // this is the case we want to handle specially
        working.javaClass != base.javaClass
      }

    private val Class<*>.isFinal: Boolean
      get() = Modifier.isFinal(modifiers)

    private val Class<*>.isCollection: Boolean
      get() = Collection::class.java.isAssignableFrom(this)

    private val Class<*>.isMap: Boolean
      get() = Map::class.java.isAssignableFrom(this)
  }
}
