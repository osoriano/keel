package com.netflix.spinnaker.keel.verification

/**
 * The set of well-known parameters we always inject into verification tests.
 */
enum class StandardTestParameter {
  TEST_ENV,
  TEST_REPO_URL,
  TEST_BUILD_NUMBER,
  TEST_ARTIFACT_VERSION,
  TEST_BRANCH_NAME,
  TEST_COMMIT_SHA,
  TEST_COMMIT_URL,
  TEST_PR_NUMBER,
  TEST_PR_URL,
  TEST_EUREKA_VIP,
  TEST_EUREKA_CLUSTER,
  TEST_LOAD_BALANCER
}

val STANDARD_TEST_PARAMETERS = StandardTestParameter.values().map { it.name }.toTypedArray()
