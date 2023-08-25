package com.netflix.spinnaker.keel.telemetry

import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.ResourceKind
import com.netflix.spinnaker.keel.api.Verification
import com.netflix.spinnaker.keel.api.artifacts.ArtifactType
import com.netflix.spinnaker.keel.api.constraints.ConstraintStatus
import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.postdeploy.PostDeployAction
import com.netflix.spinnaker.keel.core.api.EnvironmentArtifactVeto
import com.netflix.spinnaker.keel.lifecycle.LifecycleEventType
import java.time.Duration
import java.time.Instant

sealed class TelemetryEvent

data class ResourceCheckSkipped(
  val kind: ResourceKind,
  val id: String,
  val skipper: String = "unknown"
) : TelemetryEvent()

data class AboutToBeChecked(
  val lastCheckedAt: Instant,
  val type: String,
  val identifier: String? = null
)

data class ArtifactVersionUpdated(
  val name: String,
  val type: ArtifactType
) : TelemetryEvent()

data class ArtifactVersionApproved(
  val application: String,
  val deliveryConfigName: String,
  val environmentName: String,
  val artifactName: String,
  val artifactType: ArtifactType,
  val artifactVersion: String
) : TelemetryEvent()

data class EnvironmentCheckComplete(
  val application: String,
  val deliveryConfigName: String,
  val duration: Duration
)

data class ArtifactCheckSkipped(
  val type: ArtifactType,
  val name: String,
  val reason: String = "unknown"
)

data class ArtifactVersionVetoed(
  val application: String,
  val veto: EnvironmentArtifactVeto,
  val deliveryConfig: DeliveryConfig
) : TelemetryEvent()

data class AgentInvocationComplete(
  val duration: Duration,
  val agentName: String
) : TelemetryEvent()

data class VerificationStarted(
  val application: String,
  val deliveryConfigName: String,
  val environmentName: String,
  val artifactName: String,
  val artifactType: ArtifactType,
  val artifactVersion: String,
  val verificationType: String
) : TelemetryEvent() {
  constructor(context: ArtifactInEnvironmentContext, verification: Verification) : this(
    context.deliveryConfig.application,
    context.deliveryConfig.name,
    context.environmentName,
    context.artifact.name,
    context.artifact.type,
    context.version,
    verification.type
  )
}

data class VerificationCompleted(
  val application: String,
  val deliveryConfigName: String,
  val environmentName: String,
  val artifactReference: String,
  val artifactType: ArtifactType,
  val artifactVersion: String,
  val verificationType: String,
  val verificationId: String,
  val status: ConstraintStatus,
  val metadata: Map<String,Any?>
) : TelemetryEvent() {
  constructor(
    context: ArtifactInEnvironmentContext,
    verification: Verification,
    status: ConstraintStatus,
    metadata: Map<String, Any?>
  ) : this(
    context.deliveryConfig.application,
    context.deliveryConfig.name,
    context.environmentName,
    context.artifact.reference,
    context.artifact.type,
    context.version,
    verification.type,
    verification.id,
    status,
    metadata
  )
}

data class InvalidVerificationIdSeen(
  val id: String,
  val application: String,
  val deliveryConfigName: String,
  val environmentName: String,
)

data class PostDeployActionCompleted(
  val application: String,
  val deliveryConfigName: String,
  val environmentName: String,
  val artifactReference: String,
  val artifactType: ArtifactType,
  val artifactVersion: String,
  val postDeployActionType: String,
  val postDeployActionTypeId: String,
  val status: ConstraintStatus,
  val metadata: Map<String,Any?>
) : TelemetryEvent() {
  constructor(
    context: ArtifactInEnvironmentContext,
    action: PostDeployAction,
    status: ConstraintStatus,
    metadata: Map<String, Any?>
  ) : this(
    context.deliveryConfig.application,
    context.deliveryConfig.name,
    context.environmentName,
    context.artifact.reference,
    context.artifact.type,
    context.version,
    action.type,
    action.type, // todo eb: needed???
    status,
    metadata
  )
}

data class PostDeployActionStarted(
  val application: String,
  val deliveryConfigName: String,
  val environmentName: String,
  val artifactName: String,
  val artifactType: ArtifactType,
  val artifactVersion: String,
  val verificationType: String
) : TelemetryEvent() {
  constructor(context: ArtifactInEnvironmentContext, action: PostDeployAction) : this(
    context.deliveryConfig.application,
    context.deliveryConfig.name,
    context.environmentName,
    context.artifact.name,
    context.artifact.type,
    context.version,
    action.type
  )
}
