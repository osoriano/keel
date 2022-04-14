package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.kork.exceptions.UserException

/**
 * The regex provided produced too many capture groups.
 */
class InvalidRegexException(
  val regex: String,
  val tag: String
) : UserException("The provided regex ($regex) produced did not produce one capture group on tag $tag")