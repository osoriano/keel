package com.netflix.spinnaker.keel.export

import com.netflix.spinnaker.keel.front50.model.ChapCanaryAnalysisThresholds
import com.netflix.spinnaker.keel.front50.model.ChapCanaryConstraints
import com.netflix.spinnaker.keel.front50.model.ChapCanaryParameters
import com.netflix.spinnaker.keel.front50.model.ChapCanaryRegionMetadata
import com.netflix.spinnaker.keel.front50.model.ChapCanaryResultStrategy
import com.netflix.spinnaker.keel.front50.model.ChapCanaryStage

val canaryStage = ChapCanaryStage(
  refId = "15",
  name = "Canary",
  pipelineParameters = ChapCanaryParameters(
    app = "subscriptiondata",
    regionMetadata = listOf (
      ChapCanaryRegionMetadata(region = "us-east-1")
      ),
    cluster = "subscriptiondata",
    constraints = ChapCanaryConstraints(
      resultStrategy = ChapCanaryResultStrategy.LOWEST,
      canaryAnalysisThresholds = ChapCanaryAnalysisThresholds(
        pass = 80,
        marginal = 50
      )
    ),
    intervalInMinutes = 60,
    slidingWindow = false,
    numberOfInstances = 3,
    kayentaConfigIds = listOf("aa29472f-77fa-450b-a210-5348eafc1d72"),
    slidingWindowSize = 20,
    warmupInMinutes = 5,
    durationInMinutes = 60,
    env = "prod"
  )
)
