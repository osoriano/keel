package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.ArtifactStatus
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.parseAppVersion
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isSuccess
import strikt.assertions.startsWith

internal class ArtifactNormalizationTests {
  private val publishedDeb = PublishedArtifact(
    type = "DEB",
    customKind = false,
    name = "fnord",
    version = "0.156.0-h58.f67fe09",
    reference = "debian-local:pool/f/fnord/fnord_0.156.0-h58.f67fe09_all.deb",
    metadata = mapOf("releaseStatus" to ArtifactStatus.FINAL, "buildNumber" to "58", "commitId" to "f67fe09", "branch" to "main"),
    provenance = "https://my.jenkins.master/jobs/fnord-release/58"
  )

  private val normalizedDeb = publishedDeb.normalized()

  @Test
  fun `normalized artifact adds package name to version`() {
    expectThat(publishedDeb.version).not().startsWith("${publishedDeb.name}-")
    expectThat(normalizedDeb.version).startsWith("${publishedDeb.name}-")
  }

  @Test
  fun `normalized artifact version can be parsed with Frigga`() {
    expectCatching {
      normalizedDeb.version.parseAppVersion()
    }.isSuccess()
  }
}
