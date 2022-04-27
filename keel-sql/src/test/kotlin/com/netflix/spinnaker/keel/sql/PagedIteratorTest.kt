package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.config.FeatureToggles
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import io.mockk.every
import io.mockk.mockk
import org.jooq.Cursor
import org.jooq.Record1
import org.junit.jupiter.api.Test
import org.springframework.core.env.StandardEnvironment
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isFalse

class PagedIteratorTest {

  private val sqlRetry = SqlRetry(SqlRetryProperties(), FeatureToggles(StandardEnvironment()))

  private val mapper: (Record1<String>) -> String = { it.value1() }

  @Test
  fun `empty cursor`() {
    val cursor = mockk<Cursor<Record1<String>>> {
      every { hasNext() } returns false
      every { fetchNext(any()) } throws NoSuchElementException("no item")
    }

    val subject = PagedIterator(sqlRetry, cursor, 10, mapper)

    expectThat(subject.hasNext()).isFalse()
    expectThrows<NoSuchElementException> { subject.next() }
  }
}
