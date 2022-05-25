package com.netflix.spinnaker.keel.apidocs

import arrow.core.filterIsInstance
import com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.MissingNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.netflix.spinnaker.keel.api.Constraint
import com.netflix.spinnaker.keel.api.Locatable
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.VersionedArtifactProvider
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.ec2.SecurityGroupRule
import com.netflix.spinnaker.keel.api.support.ExtensionRegistry
import com.netflix.spinnaker.keel.api.support.ExtensionType
import com.netflix.spinnaker.keel.api.support.JvmExtensionType
import com.netflix.spinnaker.keel.api.support.extensionsOf
import com.netflix.spinnaker.keel.artifacts.DebianArtifact
import com.netflix.spinnaker.keel.artifacts.DockerArtifact
import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig
import com.netflix.spinnaker.keel.docker.ContainerProvider
import com.netflix.spinnaker.keel.k8s.KubernetesExtensionType
import com.netflix.spinnaker.keel.k8s.KubernetesSchemaCache
import com.netflix.spinnaker.keel.schema.Generator
import com.netflix.spinnaker.keel.schema.generateSchema
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.MOCK
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.doesNotContain
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import strikt.assertions.one
import strikt.jackson.at
import strikt.jackson.booleanValue
import strikt.jackson.findValuesAsText
import strikt.jackson.has
import strikt.jackson.isArray
import strikt.jackson.isMissing
import strikt.jackson.isObject
import strikt.jackson.isTextual
import strikt.jackson.path
import strikt.jackson.textValue
import strikt.jackson.textValues
import kotlin.reflect.KClass

@SpringBootTest(webEnvironment = MOCK)
class ApiDocTests
@Autowired constructor(
  val generator: Generator,
  val extensionRegistry: ExtensionRegistry,
  kubernetesSchemaCache: KubernetesSchemaCache
) {
  init {
    kubernetesSchemaCache.register(checkNotNull(javaClass.getResourceAsStream("/k8s/connection.yml")))
  }

  val resourceSpecTypes
    get() = extensionRegistry.extensionsOf<ResourceSpec>()

  val constraintTypes
    get() = extensionRegistry.extensionsOf<Constraint>().values.toList()

  val securityGroupRuleTypes
    get() = extensionRegistry.extensionsOf<SecurityGroupRule>().values

  val containerProviderTypes
    get() = ContainerProvider::class.sealedSubclasses.map(KClass<*>::java)

  val artifactTypes
    get() = extensionRegistry.extensionsOf<DeliveryArtifact>().values.toList()

  lateinit var api: Assertion.Builder<JsonNode>

  @BeforeEach
  fun generateSchema() {
    val schema = generator.generateSchema<SubmittedDeliveryConfig>()
      .also {
        jacksonObjectMapper()
          .setSerializationInclusion(NON_NULL)
          .enable(INDENT_OUTPUT)
          .writeValueAsString(it)
          .also(::println)
      }
      .let { jacksonObjectMapper().valueToTree<JsonNode>(it) }
    api = expectThat(schema).describedAs("API Docs response")
  }

  @Test
  fun `Does not contain a schema for ResourceKind`() {
    api.at("/\$defs/ResourceKind")
      .isMissing()
  }

  @Test
  fun `SubmittedResource kind is defined as one of the possible resource kinds`() {
    api.at("/\$defs/SubmittedResource/properties/kind/enum")
      .isArray()
      .textValues()
      .containsExactlyInAnyOrder(extensionRegistry.extensionsOf<ResourceSpec>().keys)
  }

  @TestFactory
  fun `contains a sub-schema predicate for each SubmittedResource kind`() =
    resourceSpecTypes
      .mapValues { it.value.toString() }
      .map { (kind, specSubType) ->
        dynamicTest("contains a sub-schema for SubmittedResource predicated on a kind of $kind") {
          api.at("/\$defs/SubmittedResource/allOf")
            .isArray()
            .one {
              path("if")
                .path("properties")
                .path("kind")
                .path("const")
                .textValue()
                .isEqualTo(kind)
            }
        }
      }

  @TestFactory
  fun `contains a sub-schema for each JVM SubmittedResource kind`() =
    resourceSpecTypes
      .filterIsInstance<String, JvmExtensionType>()
      .mapValues { it.value.type.simpleName }
      .map { (kind, specSubType) ->
        dynamicTest("contains a sub-schema for SubmittedResource with a spec of $specSubType") {
          api.at("/\$defs/SubmittedResource/allOf")
            .isArray()
            .one {
              path("then")
                .path("properties")
                .path("spec")
                .isObject()
                .path("\$ref")
                .textValue()
                .isEqualTo("#/\$defs/${specSubType}")
            }
        }
      }

  @TestFactory
  fun `contains a sub-schema for each Kubernetes SubmittedResource kind`() =
    resourceSpecTypes
      .filterIsInstance<String, KubernetesExtensionType>()
      .mapValues { it.value.kind }
      .map { (kind, specSubType) ->
        dynamicTest("contains a sub-schema for SubmittedResource with a spec of $specSubType") {
          api.at("/\$defs/SubmittedResource/allOf")
            .isArray()
            .one {
              path("then")
                .path("properties")
                .path("spec")
                .isObject()
                .path("\$ref")
                .textValue()
                .isEqualTo("#/\$defs/${specSubType}")
            }
        }
      }

  @Test
  fun `contains a schema for Constraint with all sub-types`() {
    api.at("/\$defs/Constraint")
      .isObject()
      .path("oneOf")
      .isArray()
      .findValuesAsText("\$ref")
      .containsExactlyInAnyOrder(
        "#/\$defs/DependsOnConstraint",
        "#/\$defs/ManualJudgementConstraint",
        "#/\$defs/PipelineConstraint",
        "#/\$defs/ImageExistsConstraint",
        "#/\$defs/AllowedTimesConstraint"
      )
  }

  @TestFactory
  fun `each Constraint sub-type has its own schema`() =
    constraintTypes
      .map(ExtensionType::toString)
      .map { type ->
        dynamicTest("Constraint sub-type $type has its own schema") {
          api.at("/\$defs/$type")
            .isObject()
        }
      }

  @Test
  fun `contains a schema for DeliveryArtifact with all sub-types`() {
    api.at("/\$defs/DeliveryArtifact")
      .isObject()
      .path("oneOf")
      .isArray()
      .findValuesAsText("\$ref")
      .containsExactlyInAnyOrder(artifactTypes.map { "#/\$defs/$it" })
  }

  @TestFactory
  fun `each DeliveryArtifact sub-type has its own schema`() =
    listOf(
      DebianArtifact::class,
      DockerArtifact::class
    )
      .map(KClass<*>::simpleName)
      .map { type ->
        dynamicTest("DeliveryArtifact sub-type $type has its own schema") {
          api.at("/\$defs/$type")
            .isObject()
        }
      }

  @TestFactory
  fun `each SecurityGroupRule sub-type has its own schema`() =
    securityGroupRuleTypes.map(ExtensionType::toString)
      .map { type ->
        dynamicTest("SecurityGroupRule sub-type $type has its own schema") {
          api.at("/\$defs/$type")
            .isObject()
        }
      }

  @Test
  fun `schema for SecurityGroupRule is oneOf the sub-types`() {
    api.at("/\$defs/SecurityGroupRule")
      .isObject()
      .has("oneOf")
      .path("oneOf")
      .isArray()
      .findValuesAsText("\$ref")
      .containsExactlyInAnyOrder(
        securityGroupRuleTypes.map { "#/\$defs/$it" }
      )
  }

  @TestFactory
  fun `each ContainerProvider sub-type has its own schema`() =
    containerProviderTypes.map(Class<*>::getSimpleName)
      .map { type ->
        dynamicTest("ContainerProvider sub-type $type has its own schema") {
          api.at("/\$defs/$type")
            .isObject()
        }
      }

  @Test
  fun `schema for ContainerProvider is oneOf the sub-types`() {
    api.at("/\$defs/ContainerProvider")
      .isObject()
      .has("oneOf")
      .path("oneOf")
      .isArray()
      .findValuesAsText("\$ref")
      .containsExactlyInAnyOrder(
        containerProviderTypes.map { "#/\$defs/${it.simpleName}" }
      )
  }

  @Test
  fun `schemas for DeliveryArtifact sub-types specify the fixed discriminator value`() {
    api.at("/\$defs/DebianArtifact/properties/type/const")
      .textValue()
      .isEqualTo("deb")
    api.at("/\$defs/DockerArtifact/properties/type/const")
      .textValue()
      .isEqualTo("docker")
  }

  @Test
  fun `data class parameters without default values are required`() {
    api.at("/\$defs/SubmittedResource/required")
      .isArray()
      .textValues()
      .containsExactlyInAnyOrder("kind")
  }

  @Test
  fun `data class parameters with default values are not required`() {
    api.at("/\$defs/SubmittedResource/required")
      .isArray()
      .textValues()
      .doesNotContain("metadata")
  }

  @Test
  fun `nullable data class parameters without default values are not required`() {
    api.at("/\$defs/SecurityGroupSpec/required")
      .isArray()
      .textValues()
      .doesNotContain("description")
  }

  @Test
  fun `prefers @JsonCreator properties to default constructor`() {
    api.at("/\$defs/ClusterSpec/required")
      .isArray()
      .textValues()
      .containsExactly("moniker")
  }

  @Test
  fun `duration properties are references to the duration schema`() {
    api.at("/\$defs/RedBlack/properties/delayBeforeDisable/\$ref")
      .textValue()
      .isEqualTo("#/\$defs/Duration")
  }

  @Test
  fun `duration schema is a patterned string`() {
    api.at("/\$defs/Duration")
      .and {
        path("type").textValue().isEqualTo("string")
        path("pattern").isTextual()
      }
  }

  @Test
  fun `non-nullable properties are marked as non-nullable in the schema`() {
    api.at("/\$defs/Moniker/properties/app/nullable")
      .booleanValue()
      .isFalse()
  }

  @Test
  fun `a class annotated with @Description can have a description`() {
    api.at("/description")
      .isTextual()
  }

  @Disabled
  @Test()
  fun `annotated class description is inherited`() {
    api.at("/\$defs/ClusterSpecSubmittedResource/description")
      .isTextual()
  }

  @Test
  fun `a property annotated with @Description can have a description`() {
    api.at("/properties/serviceAccount/description")
      .isTextual()
  }

  @Disabled
  @Test
  fun `annotated property description is inherited`() {
    api.at("/\$defs/ClusterSpecSubmittedResource/properties/spec/description")
      .isTextual()
  }

  @Test
  fun `IngressPorts are either an enum or an object`() {
    api.at("/\$defs/IngressPorts/oneOf")
      .isArray()
      .one {
        path("\$ref").textValue().isEqualTo("#/\$defs/PortRange")
      }
      .one {
        path("const").isTextual().textValue().isEqualTo("ALL")
      }
  }

  @TestFactory
  fun `locations property of Locatable types is optional`() =
    resourceSpecTypes
      .values
      .filterIsInstance<JvmExtensionType>()
      .filter { Locatable::class.java.isAssignableFrom(it.type) }
      .map(ExtensionType::toString)
      .map { locatableType ->
        dynamicTest("locations property of $locatableType is optional") {
          api.at("/\$defs/$locatableType/required")
            .isArray()
            .doesNotContain("locations")
        }
      }

  @Test
  fun `property with type Map of String to Any? does not restrict the value type to object`() {
    api.at("/\$defs/SubmittedResource/properties/metadata/additionalProperties")
      .isA<BooleanNode>()
      .booleanValue()
      .isTrue()
  }

  @TestFactory
  fun `ClusterSpec does not contain transient properties`() =
    listOf(
      VersionedArtifactProvider::artifactName.name,
      VersionedArtifactProvider::artifactType.name,
      VersionedArtifactProvider::artifactVersion.name,
    ).flatMap { propertyName ->
      listOf(
        dynamicTest("ClusterSpec does not contain transient property $propertyName used for image resolution") {
          api.at("/\$defs/ClusterSpec/properties/$propertyName")
            .isA<MissingNode>()
        },

        dynamicTest("TitusClusterSpec does not contain transient property $propertyName used for image resolution") {
          api.at("/\$defs/TitusClusterSpec/properties/$propertyName")
            .isA<MissingNode>()
        }
      )
    }

  @Test
  fun `Kubernetes CRD schemas are imported`() {
    api.at("/\$defs/Connection").isA<ObjectNode>()
  }

  @Test
  fun `KubernetesResourceSpec is not added to the schema directly as it is implemented by CRDs`() {
    api.at("/\$defs/KubernetesResourceSpec").isA<MissingNode>()
  }
}
