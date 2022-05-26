package com.netflix.spinnaker.keel.serialization

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.BeanProperty
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.InjectableValues
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer
import com.netflix.spinnaker.keel.api.*
import com.netflix.spinnaker.keel.api.deserialization.SubmittedEnvironmentPreProcessor
import com.netflix.spinnaker.keel.api.postdeploy.PostDeployAction
import com.netflix.spinnaker.keel.core.api.SubmittedEnvironment
import com.netflix.spinnaker.keel.core.api.SubmittedResource
import org.springframework.stereotype.Component
import javax.annotation.PostConstruct

@Component
private class SubmittedEnvironmentDeserializerConfig(
  private var submittedEnvironmentPreProcessors: List<SubmittedEnvironmentPreProcessor> = emptyList()
) {
  @PostConstruct
  private fun init() {
    SubmittedEnvironmentDeserializer.submittedEnvironmentPreProcessors = submittedEnvironmentPreProcessors
  }
}

/**
 * Deserializer that allows us to propagate values such as [SubmittedEnvironment.locations] to all
 * resources in the environment without having to make the corresponding properties in the resource
 * specs nullable and continually have to look up the environment.
 */
class SubmittedEnvironmentDeserializer(
) : StdNodeBasedDeserializer<SubmittedEnvironment>(SubmittedEnvironment::class.java) {
  companion object {
    var submittedEnvironmentPreProcessors: List<SubmittedEnvironmentPreProcessor> = mutableListOf()
  }

  override fun convert(root: JsonNode, context: DeserializationContext): SubmittedEnvironment =
    with(context.mapper) {
      val rawSubmittedEnvironment = convertValue(root, object : TypeReference<Map<String, Any>>() {}).toMutableMap()
      submittedEnvironmentPreProcessors.forEach {
        it.process(rawSubmittedEnvironment)
      }

      val processedRoot: JsonNode = valueToTree(rawSubmittedEnvironment)

      val name = processedRoot.path("name").textValue()
      val constraints: Set<Constraint> = convert(processedRoot, "constraints") ?: emptySet()
      val verifyWith: List<Verification> = convert(processedRoot, "verifyWith") ?: emptyList()
      val notifications: Set<NotificationConfig> = convert(processedRoot, "notifications") ?: emptySet()
      val postDeploy: List<PostDeployAction> = convert(processedRoot, "postDeploy") ?: emptyList()
      val locations: SubnetAwareLocations? = convert(processedRoot, "locations")

      val resources: Set<SubmittedResource<*>> = copy().run {
        injectableValues = InjectableLocations(locations)
        convert(processedRoot, "resources") ?: emptySet()
      }
      try {
        // what happens to the submitted environment after here ... ? should it include workload type or not?
        SubmittedEnvironment(name, resources, constraints, verifyWith, notifications, postDeploy, locations)
      } catch (e: Exception) {
        throw context.instantiationException<SubmittedEnvironment>(e)
      }
    }
}

private class InjectableLocations(
  value: SubnetAwareLocations?
) : InjectableValues.Std(mapOf("locations" to value)) {
  override fun findInjectableValue(
    valueId: Any,
    context: DeserializationContext,
    forProperty: BeanProperty,
    beanInstance: Any?
  ): Any? {
    val value = super.findInjectableValue(valueId, context, forProperty, beanInstance) as? SubnetAwareLocations
    return when {
      value == null -> null
      forProperty.type.isTypeOrSubTypeOf(SimpleLocations::class.java) -> value.toSimpleLocations()
      else -> value
    }
  }
}
