package com.netflix.spinnaker.keel.api

/**
 * Common interface for [ResourceSpec]s that represent resources that can be the target of artifact deployments.
 */
interface DeployableResourceSpec :
  ResourceSpec, VersionedArtifactProvider, ArtifactReferenceProvider
