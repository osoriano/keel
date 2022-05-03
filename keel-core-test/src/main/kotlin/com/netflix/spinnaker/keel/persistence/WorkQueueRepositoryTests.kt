package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.scm.CommitCreatedEvent
import com.netflix.spinnaker.keel.services.doInParallel
import com.netflix.spinnaker.time.MutableClock
import org.junit.jupiter.api.Test
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.util.Collections

abstract class WorkQueueRepositoryTests<IMPLEMENTATION: WorkQueueRepository> {

  abstract fun createSubject(): IMPLEMENTATION

  val clock = MutableClock()
  val subject: IMPLEMENTATION by lazy { createSubject() }

  val publishedArtifact = PublishedArtifact(
    name = "test",
    type = "DEB",
    reference = "test",
    version = "1.1",
    metadata = mapOf("branch" to "main", "commitId" to "f80cfcfdec37df59604b2ef93dfb29bade340791", "buildNumber" to "43")
  )
  val codeEvent = CommitCreatedEvent(repoKey = "stash/project/repo", targetProjectKey = "project", targetRepoSlug = "repo", targetBranch =  "main", commitHash = "hash123")

  @Test
  fun `initial queue size is 0`() {
    expectThat(subject.queueSize()).isEqualTo(0)
  }

  @Test
  fun `can add and remove an artifact event`() {
    subject.addToQueue(publishedArtifact)
    expectThat(subject.queueSize()).isEqualTo(1)

    val pendingArtifacts = subject.removeArtifactsFromQueue(1)
    expectThat(pendingArtifacts).hasSize(1)
    expectThat(pendingArtifacts.first()).isEqualTo(publishedArtifact)
    expectThat(subject.queueSize()).isEqualTo(0)
  }

  @Test
  fun `can add and remove a code event`() {
    subject.addToQueue(codeEvent)
    expectThat(subject.queueSize()).isEqualTo(1)

    val pendingCodeEvents = subject.removeCodeEventsFromQueue(1)
    expectThat(pendingCodeEvents).hasSize(1)
    expectThat(pendingCodeEvents.first()).isEqualTo(codeEvent)
    expectThat(subject.queueSize()).isEqualTo(0)
  }

  @Test
  fun `removing code events many times in parallel produces unique results`() {
    val results = Collections.synchronizedList<String>(mutableListOf())
    repeat(100) {
      subject.addToQueue(codeEvent.copy(commitHash = randomUID().toString()))
    }
    doInParallel(100) {
      subject.removeCodeEventsFromQueue(1)
        .let { event ->
          results.add(event.first().commitHash ?: "none")
        }
    }
    expect {
      that(results.size).isEqualTo(100)
      that(results.toSet().size).isEqualTo(100) // checks uniqueness
      that(subject.queueSize()).isEqualTo(0)
    }
  }

  @Test
  fun `removing artifacts many times in parallel produces unique results`() {
    val results = Collections.synchronizedList<String>(mutableListOf())
    repeat(100) {
      subject.addToQueue(publishedArtifact.copy(version = randomUID().toString()))
    }
    doInParallel(100) {
      subject.removeArtifactsFromQueue(1)
        .let { event ->
          results.add(event.first().version)
        }
    }
    expect {
      that(results.size).isEqualTo(100)
      that(results.toSet().size).isEqualTo(100) // checks uniqueness
      that(subject.queueSize()).isEqualTo(0)
    }
  }
}
