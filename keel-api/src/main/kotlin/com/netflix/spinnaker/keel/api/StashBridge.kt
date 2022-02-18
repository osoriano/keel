package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.keel.api.migration.MigrationCommitData

/**
 * This is a bridge to calling stash endpoints directly .
 */
interface StashBridge {

  suspend fun createCommitAndPrFromConfig(
    migrationCommitData: MigrationCommitData
  ): String?
}
