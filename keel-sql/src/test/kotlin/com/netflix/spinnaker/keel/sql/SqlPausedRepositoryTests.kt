/*
 *
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.keel.persistence.PausedRepositoryTests
import com.netflix.spinnaker.kork.sql.config.RetryProperties
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import com.netflix.spinnaker.kork.sql.test.SqlTestUtil
import io.mockk.every
import io.mockk.mockk
import java.time.Clock

internal object SqlPausedRepositoryTests : PausedRepositoryTests<SqlPausedRepository>() {
  private val jooq = testDatabase.context
  private val retryProperties = RetryProperties(1, 0)
  private val featureToggles: FeatureToggles = mockk(relaxed = true) {
    every { isEnabled(FeatureToggles.USE_READ_REPLICA, any()) } returns true
  }
  private val sqlRetry = SqlRetry(SqlRetryProperties(retryProperties, retryProperties), featureToggles)

  override fun factory(): SqlPausedRepository =
    SqlPausedRepository(jooq, sqlRetry, Clock.systemUTC())

  override fun SqlPausedRepository.flush() {
    SqlTestUtil.cleanupDb(jooq)
  }
}
