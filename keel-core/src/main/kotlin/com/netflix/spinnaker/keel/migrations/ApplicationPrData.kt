package com.netflix.spinnaker.keel.migrations

import com.netflix.spinnaker.keel.core.api.SubmittedDeliveryConfig


/**
 * An Object containing the data needed in order to open a PR for an application with a config file.
 */
data class ApplicationPrData (
  val deliveryConfig: SubmittedDeliveryConfig,
  val repoSlug: String,
  val projectKey: String
)
