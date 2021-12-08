package com.netflix.spinnaker.keel.front50.model

data class DisablePipeline (
  val application: String,
  val disabled: Boolean
)
