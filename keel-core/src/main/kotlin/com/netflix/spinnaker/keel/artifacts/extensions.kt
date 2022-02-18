package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact

fun PublishedArtifact.repoUrl(gitUrlPrefix: String?): String =
  gitUrlPrefix + gitMetadata?.project + "/" + gitMetadata?.repo?.name + ".git"
