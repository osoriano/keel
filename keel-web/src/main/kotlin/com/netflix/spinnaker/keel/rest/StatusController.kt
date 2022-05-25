package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.keel.api.ArtifactInEnvironmentContext
import com.netflix.spinnaker.keel.api.action.ActionRepository
import com.netflix.spinnaker.keel.api.action.ActionType.VERIFICATION
import com.netflix.spinnaker.keel.persistence.KeelRepository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.servlet.http.HttpServletResponse

/**
 * Provides various status endpoints.
 */
@RestController
@RequestMapping("/status")
class StatusController(
  private val repository: KeelRepository,
  private val actionRepository: ActionRepository
) {
  @GetMapping("/verifications/badge")
  fun getVerificationStatusBadge(response: HttpServletResponse) {
    val (verificationStatus, color) = try {
      val config = repository.getDeliveryConfigForApplication("keel")
      val mainArtifact = config.artifacts.first { it.from?.branch?.name == "main" }
      val currentVersion = repository.getCurrentlyDeployedArtifactVersion(config, mainArtifact, "prestaging")
      val verificationContext = ArtifactInEnvironmentContext(
        deliveryConfig = config,
        environmentName = "prestaging",
        artifactReference = mainArtifact.reference,
        version = currentVersion!!.version
      )
      when (actionRepository.allPassed(verificationContext, VERIFICATION)) {
        true -> "passing" to "green"
        false -> "failing" to "red"
      }
    } catch(e: Exception) {
      "unavailable" to "lightgrey"
    }

    val badgeUrl = "https://img.shields.io/static/v1?label=verifications&message=$verificationStatus&color=$color"
    response.setHeader("Location", badgeUrl)
    response.status = 302
  }
}
