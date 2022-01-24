/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.keel.clouddriver

import com.netflix.spinnaker.keel.clouddriver.model.Certificate
import com.netflix.spinnaker.keel.clouddriver.model.Credential
import com.netflix.spinnaker.keel.clouddriver.model.Network
import com.netflix.spinnaker.keel.clouddriver.model.SecurityGroupSummary
import com.netflix.spinnaker.keel.clouddriver.model.Subnet
import com.netflix.spinnaker.keel.core.api.DEFAULT_SERVICE_ACCOUNT
import com.netflix.spinnaker.keel.exceptions.AwsAccountConfigurationException
import com.netflix.spinnaker.keel.exceptions.RegistryNotFoundException
import com.netflix.spinnaker.keel.exceptions.TitusAccountConfigurationException
import com.netflix.spinnaker.kork.exceptions.IntegrationException
import kotlinx.coroutines.runBlocking

interface CloudDriverCache {
  fun securityGroupById(account: String, region: String, id: String): SecurityGroupSummary
  fun securityGroupByName(account: String, region: String, name: String): SecurityGroupSummary
  fun networkBy(id: String): Network
  fun networkBy(name: String?, account: String, region: String): Network
  fun availabilityZonesBy(account: String, vpcId: String, purpose: String, region: String): Collection<String>
  fun subnetBy(subnetId: String): Subnet
  fun subnetBy(account: String, region: String, purpose: String): Subnet
  fun credentialBy(name: String): Credential
  fun defaultKeyPairForAccount(account: String) =
    credentialBy(account).attributes["defaultKeyPair"] as String
  fun certificateByAccountAndName(account: String, name: String): Certificate
  fun certificateByArn(arn: String): Certificate

  fun getAccountId(accountName: String): String =
    credentialBy(accountName).attributes["accountId"] as? String
      ?: throw AwsAccountConfigurationException(accountName, "accountId")

  fun getAccountEnvironment(titusAccount: String): String =
    credentialBy(titusAccount).environment

  fun getAwsAccountNameForTitusAccount(titusAccount: String): String =
    credentialBy(titusAccount).attributes["awsAccount"] as? String
      ?: throw TitusAccountConfigurationException(titusAccount, "awsAccount")

  fun getAwsAccountIdForTitusAccount(titusAccount: String): String =
    getAccountId(getAwsAccountNameForTitusAccount(titusAccount))

  fun getRegistryForTitusAccount(titusAccount: String): String =
    credentialBy(titusAccount).attributes["registry"] as? String
      ?: throw RegistryNotFoundException(titusAccount)
}

class ResourceNotFound(message: String) : IntegrationException(message)
