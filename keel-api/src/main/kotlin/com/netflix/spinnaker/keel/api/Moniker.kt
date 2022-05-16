package com.netflix.spinnaker.keel.api

import com.netflix.spinnaker.kork.exceptions.SystemException
import jdk.jfr.Description


data class Moniker(
  @Description("The application name")
  val app: String,
  val stack: String? = null,
  val detail: String? = null,
  val sequence: Int? = null
) {
  companion object {
    const val MIN_SUFFIX_LENGTH = 2
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
  fun withSuffix(suffix: String, canTruncateStack: Boolean = false, maxNameLength: Int): Moniker {
    // calculates the truncation point in the detail field based on how many characters are left of the
    // max name length after removing the current detail and accounting for empty stack and detail (which
    // cause extra dashes to be added to the name)
    var numExcessChars = toName().length + suffix.length + 1 - maxNameLength
    var newMoniker = copy(detail = listOfNotNull(detail?.dropLastOrNull(numExcessChars), suffix).joinToString("-")) // We trim the detail as needed

    numExcessChars = newMoniker.toName().length - maxNameLength
    when {
      numExcessChars <= 0 -> return newMoniker
      canTruncateStack -> { // Longer than max allowed
        // Use only the suffix and drop stack and detail
        newMoniker = newMoniker.copy(stack = suffix, detail = null)
        numExcessChars = newMoniker.toName().length - maxNameLength

        if (numExcessChars > 0 && (suffix.length - numExcessChars < MIN_SUFFIX_LENGTH)) {
          // If the name is still too long we throw an exception.
          throw InvalidMonikerException("Cannot update moniker to match the max length - ${this.toName()}")
        } else {
          // Shorten the suffix if possible
          return newMoniker.copy(stack = suffix.dropLastOrNull(numExcessChars), detail = null)
        }
      }
      else -> throw InvalidMonikerException("Cannot update moniker to match the max length - ${this.toName()}")
    }
  }
}

class InvalidMonikerException(
  override val message: String? = null,
  override val cause: Throwable? = null
) : SystemException(message, cause)
