package com.netflix.spinnaker.keel.api.support

/**
 * Registers an extension to a model so that it may be used in a delivery config.
 */
interface ExtensionRegistry {
  fun <BASE : Any> register(
    baseType: Class<BASE>,
    extensionType: Class<out BASE>,
    discriminator: String
  ) = register(baseType, JvmExtensionType(extensionType), discriminator)

  fun <BASE : Any> register(
    baseType: Class<BASE>,
    extensionType: ExtensionType,
    discriminator: String
  )

  fun <BASE : Any> extensionsOf(baseType: Class<BASE>): Map<String, ExtensionType>

  fun baseTypes(): Collection<Class<*>>
}

inline fun <reified BASE : Any> ExtensionRegistry.extensionsOf(): Map<String, ExtensionType> =
  extensionsOf(BASE::class.java)

inline fun <reified BASE : Any> ExtensionRegistry.register(
  extensionType: Class<out BASE>,
  discriminator: String
) {
  register(BASE::class.java, extensionType, discriminator)
}

inline fun <reified BASE : Any> ExtensionRegistry.register(
  extensionType: ExtensionType,
  discriminator: String
) {
  register(BASE::class.java, extensionType, discriminator)
}

interface ExtensionType {
  val type: Class<*>
}

data class JvmExtensionType(
  override val type: Class<*>
) : ExtensionType {
  override fun toString(): String = type.simpleName
}
