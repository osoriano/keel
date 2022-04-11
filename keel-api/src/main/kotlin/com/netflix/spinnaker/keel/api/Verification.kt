package com.netflix.spinnaker.keel.api

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.action.Action
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.action.ActionType.VERIFICATION

/**
 * A [Verification] is a user-specified check executed by Managed Delivery to verify that an environment is
 * in good state.
 */
@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type"
)
interface Verification : Action {
  @get:JsonIgnore
  override val actionType: ActionType
    get() = VERIFICATION
}
