package com.netflix.spinnaker.keel.core.api

import com.netflix.spinnaker.keel.api.postdeploy.PostDeployAction
import com.netflix.spinnaker.keel.api.schema.Description
import com.netflix.spinnaker.keel.api.schema.Title

/**
 * Configuration for specifying a promote candidate jar post deploy action.
 * Applies to all artifacts in the environment.
 */
@Title("Promote Java client library")
@Description("Release a client jar library after the server code has been rolled out")
class PromoteJarPostDeployAction: PostDeployAction() {
  override val type: String
    get() = "promote-candidate-jar"
  override val id: String
    get() = type
}
