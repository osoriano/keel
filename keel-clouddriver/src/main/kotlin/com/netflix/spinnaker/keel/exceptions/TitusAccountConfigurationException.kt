package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.kork.exceptions.IntegrationException

class TitusAccountConfigurationException(
  val titusAccount: String,
  val missingProperty: String
) : IntegrationException("Titus account $titusAccount misconfigured: missing value for $missingProperty")