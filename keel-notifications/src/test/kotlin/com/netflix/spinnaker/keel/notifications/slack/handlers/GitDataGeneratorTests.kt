package com.netflix.spinnaker.keel.notifications.slack.handlers

import com.netflix.spinnaker.config.BaseUrlConfig
<<<<<<< d07558a084dad969ed6803cea66edf71fb70d9de
import com.netflix.spinnaker.keel.api.ScmInfo
import com.netflix.spinnaker.keel.api.artifacts.*
=======
import com.netflix.spinnaker.keel.api.ScmBridge
import com.netflix.spinnaker.keel.api.artifacts.Commit
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
>>>>>>> 97822267f3e94067dc5c21eb03773bdd8c0fbaa9
import com.netflix.spinnaker.keel.artifacts.ArtifactVersionLinks
import com.netflix.spinnaker.keel.services.mockCacheFactory
import com.netflix.spinnaker.keel.services.mockScmInfo
import com.netflix.spinnaker.keel.notifications.slack.SlackService
import com.netflix.spinnaker.time.MutableClock
import com.slack.api.model.block.SectionBlock
import com.slack.api.model.block.composition.TextObject
import com.slack.api.model.kotlin_extension.block.withBlocks
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.every
import io.mockk.mockk
import strikt.api.expect
import strikt.api.expectThat
<<<<<<< 6344b9c61164830e91283d0e4aa9473eebb379db
import strikt.assertions.*
=======
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isTrue
>>>>>>> 4bad12cf02069553408105a86d2e1a798dd74625

class GitDataGeneratorTests : JUnit5Minutests {

  class Fixture {
    val scmBridge: ScmBridge = mockk()
    val slackService: SlackService = mockk()
    val config: BaseUrlConfig = BaseUrlConfig()
    val artifactVersionLinks = ArtifactVersionLinks(mockScmInfo(), mockCacheFactory())

    val subject = GitDataGenerator(scmBridge, config, slackService, artifactVersionLinks)

    val clock: MutableClock = MutableClock()

    val application = "keel"
    val environment = "test"
    val imageUrl = "http://image-of-something-cute"
    val altText = "notification about something"

    val artifactNoMetadata = PublishedArtifact(
      name = "keel-service",
      type = "docker",
      version = "keel-service-1234",
    )
  }

  fun tests() = rootContext<Fixture> {
    fixture {
      Fixture()
    }

    before {
      every {
        slackService.getUsernameByEmail(any())
      } returns "@keel"
    }

    context("generating scm info") {
      test("generates scm links block") {
        val blocks = withBlocks {
          section {
            subject.generateScmInfo(
              sectionBlockBuilder =  this,
              application = application,
              artifact = artifactNoMetadata,
              gitMetadata = GitMetadata(
                commit = "676fea96a33cbc774685ff8b511092d9a3809f90",
                project = "spkr",
                repo = Repo("keel", null),
                branch = "main",
                pullRequest = PullRequest("1", "https://stash/pr/1"),
                commitInfo = Commit(
                  link = "https://stash",
                  sha = "676fea96a33cbc774685ff8b511092d9a3809f90",
                  message = null
                )
              )
            )
          }
        }

        expectThat(blocks.first())
          .isA<SectionBlock>()
          .get { text }.isNotNull()

        val text: TextObject = (blocks.first() as SectionBlock).text

        expect {
          that(text.toString().contains("text=<https://stash/projects/spkr/repos/keel|spkr/keel>")).isTrue()
          that(text.toString().contains("<https://stash/projects/spkr/repos/keel/branches|main>")).isTrue()
          that(text.toString().contains("<https://stash/pr/1|PR#1>")).isTrue()
          that(text.toString().contains("<https://stash|676fea9>")).isTrue()
        }
      }
    }

    context("generating commit info") {
      test("generates something even if there is no commit info") {
        val blocks = withBlocks {
          section {
            subject.generateCommitInfo(
              sectionBlockBuilder = this,
              application = application,
              imageUrl = imageUrl,
              artifact = artifactNoMetadata,
              altText = altText,
              env = environment
            )
          }
        }

        expectThat(blocks.first())
          .isA<SectionBlock>()
          .get { text }.isNotNull()

        val text: TextObject = (blocks.first() as SectionBlock).text
        expect {
          that(text.toString().contains(artifactNoMetadata.version)).isTrue()
          that(text.toString().contains(artifactNoMetadata.reference)).isTrue()
        }
      }
    }
    context("commit message display") {
      test("hides stash generated message portion behind the button") {
        val commitMetadata = GitMetadata(
          commit = "abc123",
          commitInfo = Commit(
            sha = "abc123",
            message = "fix(notifications): put back the 'show full commit' modal\n" +
              "\n" +
              "Squashed commit of the following:\n" +
              "\n" +
              "commit 676fea96a33cbc774685ff8b511092d9a3809f90\n" +
              "Author: Emily Burns <emily@email.com>\n" +
              "Date:   Fri Jul 9 16:23:20 2021 -0700\n" +
              "\n" +
              "    fix(notifications): put back the 'show full commit' modal"
          )
        )

<<<<<<< 6344b9c61164830e91283d0e4aa9473eebb379db
        val displayMessage = subject.formatCommitMessage(commitMetadata)
=======
        val displayMessage = subject.formatMessage(commitMetadat)
>>>>>>> 4bad12cf02069553408105a86d2e1a798dd74625
        expectThat(displayMessage).isEqualTo("fix(notifications): put back the 'show full commit' modal...")
      }
    }
  }
}
