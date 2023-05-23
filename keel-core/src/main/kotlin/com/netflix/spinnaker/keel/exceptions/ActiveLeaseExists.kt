package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.keel.api.Environment
import java.time.Instant

class ActiveLeaseExists(
  environmentUid: String,
  holder: String,
  leasedAt: Instant
) : EnvironmentCurrentlyBeingActedOn("Active lease exists on ${environmentUid}: leased by $holder at $leasedAt") {}

