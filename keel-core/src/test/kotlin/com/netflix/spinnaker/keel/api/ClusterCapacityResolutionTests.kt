package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.api.ec2.Capacity.AutoScalingCapacity
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.EC2ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.EC2_CLUSTER_V1_1
import com.netflix.spinnaker.keel.api.ec2.PredefinedMetricSpecification
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.api.ec2.resolveCapacity
import com.netflix.spinnaker.keel.serialization.configuredYamlMapper
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.isA
import strikt.assertions.isFailure
import strikt.assertions.isNull
import strikt.assertions.isSuccess

class ClusterCapacityResolutionTests {
  private val mapper = configuredYamlMapper().apply {
    registerSubtypes(NamedType(ClusterSpec::class.java, EC2_CLUSTER_V1_1.kind.toString()))
  }

  @Test
  fun `can resolve autoscaling capacity when scaling policies are exclusively set by overrides`() {
    val region = "us-west-2"
    val spec = ClusterSpec(
      moniker = Moniker(
        app = "fnord"
      ),
      locations = SubnetAwareLocations(
        account = "test",
        subnet = "vpc0",
        regions = setOf(
          SubnetAwareRegionSpec(
            name = region
          )
        )
      ),
      // capacity (with no desired size) defined at the default level
      capacity = CapacitySpec(
        min = 1,
        max = 100
      ),
      overrides = mapOf(
        region to ServerGroupSpec(
          // scaling policies only defined via override
          scaling = EC2ScalingSpec(
            targetTrackingPolicies = setOf(
              TargetTrackingPolicy(
                targetValue = 85.0,
                predefinedMetricSpec = PredefinedMetricSpecification(
                  type = "CPU"
                )
              )
            )
          )
        )
      )
    )

    expectCatching {
      spec.resolveCapacity(region)
    }
      .isSuccess()
      .isA<AutoScalingCapacity>()
      .get { desired }.isNull()
  }

  @Test
  fun `can resolve autoscaling capacity when capacity is exclusively set by overrides`() {
    val region = "us-west-2"
    val spec = ClusterSpec(
      moniker = Moniker(
        app = "fnord"
      ),
      locations = SubnetAwareLocations(
        account = "test",
        subnet = "vpc0",
        regions = setOf(
          SubnetAwareRegionSpec(
            name = region
          )
        )
      ),
      // scaling policies defined at the default level
      scaling = EC2ScalingSpec(
        targetTrackingPolicies = setOf(
          TargetTrackingPolicy(
            targetValue = 85.0,
            predefinedMetricSpec = PredefinedMetricSpecification(
              type = "CPU"
            )
          )
        )
      ),
      overrides = mapOf(
        region to ServerGroupSpec(
          // capacity only defined via override
          capacity = CapacitySpec(
            min = 1,
            max = 100
          ),
        )
      )
    )

    expectCatching {
      spec.resolveCapacity(region)
    }
      .isSuccess()
      .isA<AutoScalingCapacity>()
      .get { desired }.isNull()
  }

  @Test
  fun `cannot resolve autoscaling capacity when no scaling policies exist at all`() {
    val region = "us-west-2"
    val spec = ClusterSpec(
      moniker = Moniker(
        app = "fnord"
      ),
      locations = SubnetAwareLocations(
        account = "test",
        subnet = "vpc0",
        regions = setOf(
          SubnetAwareRegionSpec(
            name = region
          )
        )
      ),
      // capacity (with no desired size) defined at the default level
      capacity = CapacitySpec(
        min = 1,
        max = 100
      )
    )

    expectCatching {
      spec.resolveCapacity(region)
    }
      .isFailure()
  }

  // note: this was an attempt to reproduce MD-219
  @Test
  fun `can resolve autoscaling capacity when capacity is set by defaults and there's no scaling policy override`() {
    val spec: ClusterSpec = """
      moniker:
        app: fnord
      artifactReference: main
      locations:
        account: test
        regions:
          - name: eu-west-1
          - name: us-east-1
      capacity:
        max: 2
        min: 2
      dependencies:
        securityGroupNames:
          - fnord
          - nf-datacenter
          - nf-infrastructure
      deployWith:
        health: AUTO
        strategy: highlander
      health:
        terminationPolicies:
          - Default
      launchConfiguration:
        associateIPv6Address: true
        instanceType: m5.xlarge
      overrides:
        us-east-1:
          scaling:
            targetTrackingPolicies:
              - disableScaleIn: true
                predefinedMetricSpec:
                  type: ASGAverageCPUUtilization
                targetValue: 55
                warmup: PT5M
      scaling:
        targetTrackingPolicies:
          - disableScaleIn: true
            predefinedMetricSpec:
              type: ASGAverageCPUUtilization
            targetValue: 55
            warmup: PT5M
      """
      .trimIndent().let {
        mapper.readValue(it)
      }

    val region = "eu-west-1"

    expectCatching {
      spec.resolveCapacity(region)
    }
      .isSuccess()
      .isA<AutoScalingCapacity>()
      .get { desired }.isNull()
  }

  @Test
  fun `can resolve autoscaling capacity with empty scaling policies in overrides`() {
    val region = "us-west-2"
    val spec = ClusterSpec(
      moniker = Moniker(
        app = "fnord"
      ),
      locations = SubnetAwareLocations(
        account = "test",
        subnet = "vpc0",
        regions = setOf(
          SubnetAwareRegionSpec(
            name = region
          )
        )
      ),
      // capacity (with no desired size) defined at the default level
      capacity = CapacitySpec(
        min = 1,
        max = 100
      ),
      // scaling policies defined at the default level
      scaling = EC2ScalingSpec(
        targetTrackingPolicies = setOf(
          TargetTrackingPolicy(
            targetValue = 85.0,
            predefinedMetricSpec = PredefinedMetricSpecification(
              type = "CPU"
            )
          )
        )
      ),
      overrides = mapOf(
        region to ServerGroupSpec(
          // default scaling spec has empty policies
          scaling = EC2ScalingSpec()
        )
      )
    )

    expectCatching {
      spec.resolveCapacity(region)
    }
      .isSuccess()
      .isA<AutoScalingCapacity>()
      .get { desired }.isNull()
  }
}
