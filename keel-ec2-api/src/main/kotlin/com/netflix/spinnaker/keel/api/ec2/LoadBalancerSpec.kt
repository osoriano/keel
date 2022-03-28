package com.netflix.spinnaker.keel.api.ec2

import com.netflix.spinnaker.keel.api.SpinnakerResourceSpec
import com.netflix.spinnaker.keel.api.SubnetAwareLocations
import java.time.Duration

interface LoadBalancerSpec : SpinnakerResourceSpec<SubnetAwareLocations> {
  val loadBalancerType: LoadBalancerType
  override val locations: SubnetAwareLocations
  val internal: Boolean
  val dependencies: LoadBalancerDependencies
  val idleTimeout: Duration
}
