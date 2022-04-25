package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.ec2.Capacity.AutoScalingCapacity
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.CapacitySpec
import com.netflix.spinnaker.keel.api.ec2.ClusterSpec.ServerGroupSpec
import com.netflix.spinnaker.keel.api.ec2.EC2ScalingSpec
import com.netflix.spinnaker.keel.api.ec2.PredefinedMetricSpecification
import com.netflix.spinnaker.keel.api.ec2.TargetTrackingPolicy
import com.netflix.spinnaker.keel.api.ec2.resolveCapacity
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.assertions.isA
import strikt.assertions.isFailure
import strikt.assertions.isSuccess

class ClusterCapacityResolutionTests {

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
}
