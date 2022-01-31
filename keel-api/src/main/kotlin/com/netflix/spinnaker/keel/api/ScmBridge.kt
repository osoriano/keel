package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.migration.MigrationCommitData
import com.netflix.spinnaker.keel.api.migration.PrLink

/**
 * This is a bridge to calling Igor for scm related endpoints.
 */
interface ScmBridge{

  suspend fun getScmInfo():
    Map<String, String?>


  suspend fun createPr(scmType: String,
                       projectKey: String,
                       repoSlug: String,
                       migrationCommitData: MigrationCommitData
  ): PrLink
}
