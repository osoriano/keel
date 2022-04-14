package com.netflix.spinnaker.keel.veto

import com.netflix.spinnaker.keel.api.Resource

class DummyVeto(
  private val allowAll: Boolean
) : Veto {
  override suspend fun check(resource: Resource<*>): VetoResponse =
    if (allowAll) {
      allowedResponse()
    } else {
      deniedResponse("None shall pass")
    }
}
