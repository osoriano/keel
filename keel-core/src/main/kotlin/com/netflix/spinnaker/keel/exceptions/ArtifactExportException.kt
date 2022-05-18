package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.kork.exceptions.SystemException

open class ArtifactExportException(message: String) : SystemException(message)

class ArtifactMetadataUnavailableException(artifactType: String, artifactName: String) :
  ArtifactExportException("Unable to fetch metadata for ${artifactType.uppercase()} artifact $artifactName")

class TagToReleaseArtifactException(artifactName: String) :
    ArtifactExportException(
        "artifact $artifactName seems to be produced by a tag-to-release flow which is unsupported."
    )
