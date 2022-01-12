package com.netflix.spinnaker.keel.api

/**
 * A reference to a specific resource of a given [kind] as identified by its [name], or a set
 * of resources of the same kind when [name] is `*`.
 */
data class NamedResource(
  val kind: ResourceKind,
  val name: String
) {
  fun matches(resource: Resource<*>) =
    kind == resource.kind && (name == "*" || name == resource.name)
}
