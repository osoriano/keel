package com.netflix.spinnaker.keel.jackson

import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.DeserializationConfig
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.deser.Deserializers
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.Serializers
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.TagVersionStrategy
import com.netflix.spinnaker.keel.jackson.mixins.CommitMixin
import com.netflix.spinnaker.keel.jackson.mixins.ResourceKindMixin

fun ObjectMapper.registerKeelApiModule(): ObjectMapper = registerModule(KeelApiModule)

object KeelApiModule : SimpleModule("Keel API") {

  override fun setupModule(context: SetupContext) {
    with(context) {
      addSerializers(KeelApiSerializers)
      addDeserializers(KeelApiDeserializers)
      setMixInAnnotations<ResourceKind, ResourceKindMixin>()
      setMixInAnnotations<Commit, CommitMixin>()
      insertAnnotationIntrospector(FactoryAnnotationIntrospector())
    }
  }
}

/**
 * Any custom [JsonSerializer] implementations for `keel-api` types.
 */
internal object KeelApiSerializers : Serializers.Base() {
  override fun findSerializer(config: SerializationConfig, type: JavaType, beanDesc: BeanDescription): JsonSerializer<*>? =
    when (type.rawClass) {
      TagVersionStrategy::class.java -> TagVersionStrategySerializer
      else -> null
    }
}

/**
 * Any custom [JsonDeserializer] implementations for `keel-api` types.
 */
internal object KeelApiDeserializers : Deserializers.Base() {
  override fun findEnumDeserializer(type: Class<*>, config: DeserializationConfig, beanDesc: BeanDescription): JsonDeserializer<*>? =
    when (type) {
      TagVersionStrategy::class.java -> TagVersionStrategyDeserializer
      else -> null
    }
}

internal inline fun <reified T> NamedType(name: String) = NamedType(T::class.java, name)

internal inline fun <reified TARGET, reified MIXIN> SetupContext.setMixInAnnotations() {
  setMixInAnnotations(TARGET::class.java, MIXIN::class.java)
}
