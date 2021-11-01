package com.netflix.spinnaker.keel.titus

import com.netflix.spinnaker.keel.clouddriver.CloudDriverCache
import com.netflix.spinnaker.keel.titus.exceptions.AwsAccountConfigurationException
import com.netflix.spinnaker.keel.titus.exceptions.TitusAccountConfigurationException

/**
 * @return the name of the AWS account that underpins the Titus account [titusAccountName].
 */
fun CloudDriverCache.awsAccountNameForTitusAccount(titusAccountName: String): String =
  credentialBy(titusAccountName).attributes["awsAccount"] as? String
    ?: throw TitusAccountConfigurationException(titusAccountName, "awsAccount")

/**
 * @return the id of the AWS account that underpins the Titus account [titusAccountName].
 */
fun CloudDriverCache.awsAccountIdForTitusAccount(titusAccountName: String): String =
  awsAccountNameForTitusAccount(titusAccountName).let { awsAccountName ->
    credentialBy(awsAccountName).attributes["accountId"] as? String
      ?: throw AwsAccountConfigurationException(titusAccountName, "accountId")
  }
