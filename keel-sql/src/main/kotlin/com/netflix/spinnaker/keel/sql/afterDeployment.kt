package com.netflix.spinnaker.keel.sql

import org.jooq.Record
import org.jooq.SelectForUpdateStep
import org.jooq.SelectOptionStep

/**
 * Set a share mode lock on a select query to prevent phantom reads in a transaction.
 *
 * In MySQL 5.7, this is `LOCK IN SHARE MODE`
 * See https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
 */
fun <R : Record?> SelectForUpdateStep<R>.lockInShareMode(useLockingRead: Boolean): SelectOptionStep<R> =
  if(useLockingRead) {
    this.forShare()
  } else {
    this
  }
