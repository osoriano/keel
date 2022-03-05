package com.netflix.spinnaker.keel.persistence

import com.netflix.spinnaker.keel.api.ResourceDiff
import org.apache.commons.codec.digest.DigestUtils

/**
 * Stores a hash of the diff and a record of the number of times we've taken action to correct it
 */
interface DiffFingerprintRepository {
  fun store(entityId: String, diff: ResourceDiff<*>)

  fun markActionTaken(entityId: String)

  fun diffCount(entityId: String): Int

  fun actionTakenCount(entityId: String): Int

  fun seen(entityId: String, diff: ResourceDiff<*>): Boolean

  fun clear(entityId: String)

  fun ResourceDiff<*>.generateHash(): String =
    DigestUtils.sha1Hex(toDeltaJson().toString().toByteArray()).uppercase()
}
