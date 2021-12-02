package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.migration.MigrationCommitData
import com.netflix.spinnaker.keel.api.migration.PrLink

interface ScmBridge{

  /**
   * This is a bridge to calling Igor in order to get all configured SCM base URLs.
   */
  suspend fun getScmInfo():
    Map<String, String?>


  suspend fun createPr(scmType: String,
                       projectKey: String,
                       repoSlug: String,
                       migrationCommitData: MigrationCommitData
  ): PrLink
}
