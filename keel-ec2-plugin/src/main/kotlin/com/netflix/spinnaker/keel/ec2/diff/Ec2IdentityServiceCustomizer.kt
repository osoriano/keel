package com.netflix.spinnaker.keel.ec2.diff

import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancer
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec.Action
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec.Listener
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec.Rule
import com.netflix.spinnaker.keel.api.ec2.ApplicationLoadBalancerSpec.TargetGroup
import com.netflix.spinnaker.keel.diff.IdentityServiceCustomizer
import com.netflix.spinnaker.keel.diff.ofCollectionItems
import de.danielbechler.diff.identity.IdentityService
import org.springframework.stereotype.Component

@Component
class Ec2IdentityServiceCustomizer : IdentityServiceCustomizer {
  override fun customize(identityService: IdentityService) {
    with(identityService) {
      ofCollectionItems(ApplicationLoadBalancer::targetGroups).via { working, base ->
        (working as? TargetGroup)?.name == (base as? TargetGroup)?.name
      }
      ofCollectionItems(ApplicationLoadBalancer::listeners).via { working, base ->
        (working as? Listener to base as? Listener).let { (l, r) ->
          l?.protocol == r?.protocol && l?.port == r?.port
        }
      }
      ofCollectionItems(Listener::defaultActions).via { working, base ->
        (working as? Action to base as? Action).let { (l, r) ->
          l?.order == r?.order
        }
      }
      ofCollectionItems(Listener::rules).via { working, base ->
        (working as? Rule to base as? Rule).let { (l, r) ->
          l?.priority == r?.priority
        }
      }
      ofCollectionItems(Rule::actions).via { working, base ->
        (working as? Action to base as? Action).let { (l, r) ->
          l?.order == r?.order
        }
      }
    }
  }
}
