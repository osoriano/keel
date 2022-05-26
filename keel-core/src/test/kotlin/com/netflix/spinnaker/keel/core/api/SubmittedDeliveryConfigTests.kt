package com.netflix.spinnaker.keel.core.api

import com.netflix.spinnaker.keel.test.submittedDeliveryConfig
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.isEqualTo
import strikt.assertions.isTrue

internal class SubmittedDeliveryConfigTests {
  private val submittedConfig = submittedDeliveryConfig()

  @Test
  fun `correctly copies properties recursively when translating to DeliveryConfig`() {
    expectThat(submittedConfig.toDeliveryConfig()) {
      // basic props
      get { application }.isEqualTo(submittedConfig.application)
      get { name }.isEqualTo(submittedConfig.name)
      get { serviceAccount }.isEqualTo(submittedConfig.serviceAccount)
      get { rawConfig }.isEqualTo(submittedConfig.rawConfig)
      // artifacts
      get { artifacts.map { it.type } }
        .isEqualTo(submittedConfig.artifacts.map { it.type })
      get { artifacts.map { it.name } }
        .isEqualTo(submittedConfig.artifacts.map { it.name })
      get { artifacts.map { it.reference } }
        .isEqualTo(submittedConfig.artifacts.map { it.reference })
      get { artifacts.map { it.deliveryConfigName } }
        .isEqualTo(submittedConfig.artifacts.map { it.deliveryConfigName })
      // environments
      get { environments.map { it.name } }
        .isEqualTo(submittedConfig.environments.map { it.name })
      get { environments.map { it.constraints } }
        .isEqualTo(submittedConfig.environments.map { it.constraints })
      get { environments.map { it.verifyWith } }
        .isEqualTo(submittedConfig.environments.map { it.verifyWith })
      get { environments.map { it.postDeploy } }
        .isEqualTo(submittedConfig.environments.map { it.postDeploy })
      // resources
      get { resources.map { it.kind } }
        .isEqualTo(submittedConfig.environments.flatMap { it.resources }.map { it.kind })
      get { resources.map { it.spec } }
        .isEqualTo(submittedConfig.environments.flatMap { it.resources }.map { it.spec })
      // preview environments
      get { previewEnvironments }
        .isEqualTo(submittedConfig.previewEnvironments)
    }
  }

  @Test
  fun `correctly sets temporary flags recursively when translating to DeliveryConfig`() {
    expectThat(submittedConfig.toDeliveryConfig(isDryRun = true)) {
      get { artifacts }.all { get { isDryRun }.isTrue() }
      get { environments }.all { get { isDryRun }.isTrue() }
      get { resources }.all { get { isDryRun }.isTrue() }
    }
  }
}
