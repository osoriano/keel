package com.netflix.spinnaker.keel.scheduling.activities

import org.junit.jupiter.api.Test
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.time.Duration

class DefaultSchedulingConfigActivitiesTest {

  @Test
  fun `should use fallback duration when no config provided`() {
    val environment = StandardEnvironment()
    val subject = DefaultSchedulingConfigActivities(environment)

    environment.propertySources.addFirst(MapPropertySource("test", mapOf()))
    expectThat(subject.configOrDefault("my-config", "my-resource", Duration.ofSeconds(30))).isEqualTo(Duration.ofSeconds(30))
  }

  @Test
  fun `should use default when no resource kind config provided`() {
    val environment = StandardEnvironment()
    val subject = DefaultSchedulingConfigActivities(environment)

    environment.propertySources.addFirst(
      MapPropertySource(
        "test",
        mapOf(
          "keel.resource-scheduler.my-resource.my-config" to null,
          "keel.resource-scheduler.default.my-config" to Duration.ofHours(1)
        )
      )
    )
    expectThat(subject.configOrDefault("my-config", "my-resource", Duration.ofSeconds(30))).isEqualTo(Duration.ofHours(1))
  }

  @Test
  fun `should use resource kind config when provided`() {
    val environment = StandardEnvironment()
    val subject = DefaultSchedulingConfigActivities(environment)

    environment.propertySources.addFirst(
      MapPropertySource(
        "test",
        mapOf(
          "keel.resource-scheduler.my-resource.my-config" to Duration.ofHours(2),
          "keel.resource-scheduler.default.my-config" to Duration.ofHours(1)
        )
      )
    )
    expectThat(subject.configOrDefault("my-config", "my-resource", Duration.ofSeconds(30))).isEqualTo(Duration.ofHours(2))
  }
}
