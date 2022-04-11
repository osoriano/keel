package com.netflix.spinnaker.keel.api.postdeploy

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.netflix.spinnaker.keel.api.action.Action
import com.netflix.spinnaker.keel.api.action.ActionType
import com.netflix.spinnaker.keel.api.action.ActionType.POST_DEPLOY

@JsonTypeInfo(
  use = Id.NAME,
  include = As.EXISTING_PROPERTY,
  property = "type"
)
abstract class PostDeployAction : Action {
  override val actionType: ActionType
    get() = POST_DEPLOY
}
