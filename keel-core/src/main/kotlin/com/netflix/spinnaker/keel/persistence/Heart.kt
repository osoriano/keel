package com.netflix.spinnaker.keel.persistence

/**
 * A repository to record a heartbeat for the instance, so that
 * locks don't get prematurely terminated when the instance is still
 * alive and kicking.
 *
 * The underlying implementation runs [beat] on a schedule.
 */
interface Heart {

  /**
   * Records a heartbeat for the instance
   */
  fun beat()
}
