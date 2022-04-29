package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact

private val buildUrl = "https://krypton.builds.test.netflix.net/job/USERS-lpollo-lpollo-local-test-build/230/"

val disguisedPublishedDocker = PublishedArtifact(
  name = "See image.properties",
  type = "docker",
  reference = "image.properties",
  version = "See image.properties",
  metadata = mapOf(
    "rocketEventType" to "BUILD",
    "controllerName" to "krypton",
    "jobName" to "users-lpollo-lpollo-local-test-build",
    "buildDetail" to mapOf(
      "buildEngine" to "jenkins-krypton",
      "buildId" to "230",
      "buildDisplayName" to "USERS-lpollo-lpollo-local-test-build #230 - Branch: master",
      "buildDescription" to "msrc@netflix.com merged <a href=\"https://stash.corp.netflix.com/projects/~LPOLLO/repos/lpollo-local…",
      "result" to "SUCCESSFUL",
      "buildUrl" to buildUrl,
      "artifacts" to listOf(
        "$buildUrl/artifact/lpollo-local-test-client/build/reports/project/dependencies.txt",
        "$buildUrl/artifact/lpollo-local-test-client/build/reports/project/properties.txt",
        "$buildUrl/artifact/lpollo-local-test-proto-definition/build/reports/project/dependencies.txt",
        "$buildUrl/artifact/lpollo-local-test-proto-definition/build/reports/project/properties.txt",
        "$buildUrl/artifact/lpollo-local-test-server/build/distributions/lpollo-local-test-server_0.0.1%7Esnapshot-h230.88b3f71_all.deb",
        "$buildUrl/artifact/lpollo-local-test-server/build/image-server.properties",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/project/dependencies.txt",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/project/properties.txt",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/properties/properties-report-lpollolocaltest.txt",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/classes/com.netflix.lpollolocaltest.SmokeTest.html",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/css/base-style.css",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/css/style.css",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/index.html",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/js/report.js",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/smokeTest/packages/com.netflix.lpollolocaltest.html",
        "$buildUrl/artifact/lpollo-local-test-server/build/reports/startup/startup-report-lpollolocaltest-1.html"
      ),
      "logs" to listOf(
        "$buildUrl/consoleText"
      ),
      "reports" to emptyList<String>(),
      "buildNumber" to 230,
      "startedAt" to 1642470544511,
      "queuedAt" to 1642470544507,
      "completedAt" to 1642470752660,
      "commitId" to "88b3f7131aaea370b5649da695387e5e23c4053f",
      "agentId" to "nflx-agent-krypton-i-0ba8112ce613b7dff"
    ),
    "triggerEvent" to mapOf(
      "repoKey" to "stash/abe/xp-env",
      "source" to mapOf(
        "projectKey" to "abe",
        "repoName" to "xp-env",
        "refId" to "refs/heads/master",
        "branchName" to "master",
        "sha" to "8a946cccabf31fb8352a69e0f4d2dbfb5d73503a",
        "message" to "Merge pull request #44 in ABE/xp-env from jeffreyw/add_r_kernel to master\n\n* commit 'b6043374d8a569…",
        "url" to "https://stash.corp.netflix.com/projects/abe/repos/xp-env/commits/8a946cccabf31fb8352a69e0f4d2dbfb5d73503a",
        "author" to mapOf(
          "name" to "jeffreyw",
          "email" to "jeffreyw@netflix.com"
        ),
        "date" to "Jun 11, 2020 12:01:03 AM",
        "refUrl" to "https://stash.corp.netflix.com/projects/abe/repos/xp-env/browse?at=refs%2Fheads%2Fmaster",
        "skippingCi" to false
      ),
      "target" to mapOf(
        "projectKey" to "abe",
        "repoName" to "xp-env",
        "refId" to "refs/heads/superbranch",
        "branchName" to "super-branch",
        "sha" to "8a946cccabf31fb8352a69e0f4d2dbfb5d73503a",
        "message" to "Merge pull request #44 in ABE/xp-env from jeffreyw/add_r_kernel to master\n\n* commit 'b6043374d8a569…",
        "url" to "https://stash.corp.netflix.com/projects/abe/repos/xp-env/commits/8a946cccabf31fb8352a69e0f4d2dbfb5d73503a",
        "author" to mapOf(
          "name" to "jeffreyw",
          "email" to "jeffreyw@netflix.com"
        ),
        "date" to "Jun 11, 2020 12:01:03 AM",
        "refUrl" to "https://stash.corp.netflix.com/projects/abe/repos/xp-env/browse?at=refs%2Fheads%2Fmaster",
        "skippingCi" to false
      )
    )
  )
)
