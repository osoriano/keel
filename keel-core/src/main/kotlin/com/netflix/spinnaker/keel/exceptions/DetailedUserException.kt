package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.kork.exceptions.UserException

/**
 * A [UserException] that may optionally carry a detailed list of [errors].
 */
open class DetailedUserException(
  override val message: String,
  override val cause: Throwable? = null,
  open val errors: List<String> = emptyList()
) : UserException(message, cause)
