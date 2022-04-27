package com.netflix.spinnaker.keel.sql

import org.jooq.Cursor
import org.jooq.Record

/**
 * Provides an iterator for lazily-fetched queries.
 *
 * As an optimization, this iterator uses an internal buffer to support batching of paginated results.
 */
class PagedIterator<R : Record, T>(
  private val sqlRetry: SqlRetry,
  private val cursor: Cursor<R>,
  private val fetchSize: Int,
  private val mapper: (R) -> T
) : Iterator<T> {

  private var buffer: Iterator<T> = emptyList<T>().iterator()

  override fun hasNext(): Boolean {
    if (buffer.hasNext()) {
      return true
    }
    return sqlRetry.withRetry(RetryCategory.READ) {
      cursor.hasNext()
    }
  }

  override fun next(): T {
    if (!buffer.hasNext()) {
      buffer = sqlRetry.withRetry(RetryCategory.READ) {
        cursor.fetchNext(fetchSize)
          .map(mapper)
          .iterator()
      }
    }
    return buffer.next()
  }
}
