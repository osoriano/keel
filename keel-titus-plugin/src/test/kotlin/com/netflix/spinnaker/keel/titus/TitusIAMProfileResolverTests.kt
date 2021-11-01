package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.api.Moniker
import com.netflix.spinnaker.keel.api.SimpleLocations
import com.netflix.spinnaker.keel.api.SimpleRegionSpec
import com.netflix.spinnaker.keel.api.titus.TITUS_CLUSTER_V1
import com.netflix.spinnaker.keel.api.titus.TitusClusterSpec
import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.clouddriver.model.Credential
import com.netflix.spinnaker.keel.docker.DigestProvider
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.keel.titus.optics.titusClusterSpecIamProfileLens
import io.mockk.mockk
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import io.mockk.coEvery as every

internal class TitusIAMProfileResolverTests {
  private val titusAccount = "titustestvpc"
  private val awsAccount = "test"
  private val awsAccountId = "964923154387"
  private val iamProfileArn = "arn:aws:iam::${awsAccountId}:role/fnordInstanceProfile"

  private val specTemplate = TitusClusterSpec(
    moniker = Moniker(
      app = "fnord"
    ),
    locations = SimpleLocations(
      account = titusAccount,
      regions = setOf(SimpleRegionSpec("us-west-2"))
    ),
    container = DigestProvider(
      organization = "fnord",
      image = "docker",
      digest = "3a50e7cbbe1db6f07066698dff7f62f4"
    )
  )

  private fun TitusClusterSpec.toResource() = resource(
    kind = TITUS_CLUSTER_V1.kind,
    spec = this
  )

  private val cloudDriverCache: CloudDriverCache = mockk {
    every { credentialBy(specTemplate.locations.account) } returns Credential(
      name = titusAccount,
      type = "titus",
      environment = "test",
      attributes = mutableMapOf("awsAccount" to awsAccount)
    )
    every { credentialBy("test") } returns Credential(
      name = awsAccount,
      type = "aws",
      environment = "test",
      attributes = mutableMapOf("accountId" to awsAccountId)
    )
  }

  private val resolver = TitusIAMProfileResolver(cloudDriverCache)

  @Test
  fun `a full ARN in the spec is left alone`() {
    val resource = titusClusterSpecIamProfileLens.set(specTemplate, iamProfileArn).toResource()
    val resolved = resolver.invoke(resource)
    expectThat(resolved.spec.defaults.iamProfile) isEqualTo iamProfileArn
  }

  @Test
  fun `a profile name is resolved into a full ARN`() {
    val resource = titusClusterSpecIamProfileLens.set(specTemplate, "fnordInstanceProfile").toResource()
    val resolved = resolver.invoke(resource)
    expectThat(resolved.spec.defaults.iamProfile) isEqualTo iamProfileArn
  }

  @Test
  fun `a missing IAM profile is resolved into the default ARN`() {
    val resource = titusClusterSpecIamProfileLens.set(specTemplate, "fnordInstanceProfile").toResource()
    val resolved = resolver.invoke(resource)
    expectThat(resolved.spec.defaults.iamProfile) isEqualTo iamProfileArn
  }
}
