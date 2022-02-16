package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.migration.MigrationCommitData

/**
 * This is a bridge to calling Igor for scm related endpoints.
 */
interface ScmBridge{

  suspend fun getScmInfo():
    Map<String, String?>


  suspend fun createCommitAndPrFromConfig(
    migrationCommitData: MigrationCommitData
  ): String?
}
