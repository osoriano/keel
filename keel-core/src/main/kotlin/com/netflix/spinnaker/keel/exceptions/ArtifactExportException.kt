package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.kork.exceptions.SystemException

open class ArtifactExportException(message: String) : SystemException(message)

class ArtifactMetadataUnavailableException(artifactType: String, artifactName: String) :
  ArtifactExportException("Unable to fetch metadata for ${artifactType.uppercase()} artifact $artifactName")

class TagToReleaseArtifactException(artifact: PublishedArtifact) :
    ArtifactExportException(
      with(artifact) {
        "${type.uppercase()} artifact $name version $version seems to be produced by a tag-to-release flow which is unsupported."
      }
    )
