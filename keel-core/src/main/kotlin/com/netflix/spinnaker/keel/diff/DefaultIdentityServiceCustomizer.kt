package com.netflix.spinnaker.keel.diff

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import de.danielbechler.diff.identity.IdentityService
import org.springframework.stereotype.Component

@Component
class DefaultIdentityServiceCustomizer : IdentityServiceCustomizer {
  override fun customize(identityService: IdentityService) {
    with(identityService) {
      // These tell the differ how to match items within collections before comparing, so that it
      // doesn't interpret changes in collections as if the entire parent object is different.
      ofCollectionItems(DeliveryConfig::artifacts).via { working, base ->
        (working as? DeliveryArtifact)?.reference == (base as? DeliveryArtifact)?.reference
      }
      ofCollectionItems(DeliveryConfig::environments).via { working, base ->
        (working as? Environment)?.name == (base as? Environment)?.name
      }
      ofCollectionItems(Environment::resources).via { working, base ->
        (working as? Resource<*>)?.id == (base as? Resource<*>)?.id
      }
    }
  }
}
