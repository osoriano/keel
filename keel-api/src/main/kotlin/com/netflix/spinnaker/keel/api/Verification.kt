package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.action.Action
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.action.ActionType.VERIFICATION

/**
 * A [Verification] is a user-specified check executed by Managed Delivery to verify that an environment is
 * in good state.
 */
interface Verification : Action {
  override val actionType: ActionType
    get() = VERIFICATION
}
