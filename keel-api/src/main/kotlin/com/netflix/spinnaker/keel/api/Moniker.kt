package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.kork.exceptions.SystemException


data class Moniker(
  val app: String,
  val stack: String? = null,
  val detail: String? = null,
  val sequence: Int? = null
) {
  companion object {
    val MAX_LENGTH = 32
  }

  override fun toString(): String =
    toName()

  fun toName(): String =
    when {
      stack == null && detail == null -> app
      detail == null -> "$app-$stack"
      else -> "$app-${stack.orEmpty()}-$detail"
    }

  private fun String?.dropLastOrNull(n: Int): String? {
    if (this == null) return null
    if (n <= 0) return this
    return this.dropLast(n).let {
      it.ifEmpty { null }
    }
  }

  /**
   * @return The [Moniker] with an updated [Moniker.detail] field containing as much of the specified
   * [suffix] as possible while respecting max length constraints on resource names.
   */
  fun withSuffix(suffix: String, maxNameLength: Int = MAX_LENGTH): Moniker {
    // calculates the truncation point in the detail field based on how many characters are left of the
    // max name length after removing the current detail and accounting for empty stack and detail (which
    // cause extra dashes to be added to the name)
    var lengthDiff = toName().length + suffix.length + 1 - maxNameLength
    var newMoniker = copy(detail = listOfNotNull(detail?.dropLastOrNull(lengthDiff), suffix).joinToString("-")) // We trim the detail as needed

    lengthDiff = newMoniker.toName().length - maxNameLength
    if (lengthDiff > 0 && stack != null) {
      // If the name is still too long we trim the stack
      newMoniker = newMoniker.copy(stack = stack.dropLastOrNull(lengthDiff))
    }
    lengthDiff = newMoniker.toName().length - maxNameLength
    if (lengthDiff > 0) {
      // If the name is still too long we throw an exception. Optional improvement is to make the suffix shorter
      throw InvalidMonikerException("Cannot update moniker to match the max length - ${newMoniker.toName()}")
    }
    return newMoniker
  }
}

class InvalidMonikerException(
  override val message: String? = null,
  override val cause: Throwable? = null
) : SystemException(message, cause)
