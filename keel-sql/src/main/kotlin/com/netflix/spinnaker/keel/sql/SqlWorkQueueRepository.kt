package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.kotlin.OpenClass
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.scm.CodeEvent
import com.netflix.spinnaker.keel.artifacts.WorkQueueEventType
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.persistence.WorkQueueRepository
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.WORK_QUEUE
import com.netflix.spinnaker.keel.sql.RetryCategory.WRITE
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.slf4j.LoggerFactory
import java.time.Clock
import java.util.Collections

@OpenClass
class SqlWorkQueueRepository(
  private val jooq: DSLContext,
  private val clock: Clock,
  private val mapper: ObjectMapper,
  private val sqlRetry: SqlRetry
) : WorkQueueRepository {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override fun addToQueue(codeEvent: CodeEvent) {
    sqlRetry.withRetry(WRITE) {
      jooq.insertInto(WORK_QUEUE)
        .set(WORK_QUEUE.UID, randomUID().toString())
        .set(WORK_QUEUE.TYPE, WorkQueueEventType.CODE.name)
        .set(WORK_QUEUE.FIRST_SEEN, clock.instant())
        .set(WORK_QUEUE.JSON, mapper.writeValueAsString(codeEvent))
        .execute()
    }
  }

  override fun removeCodeEventsFromQueue(limit: Int): Collection<CodeEvent> =
    sqlRetry.withRetry(WRITE) {
      val events = mutableListOf<String>()
      jooq.inTransaction {
        select(WORK_QUEUE.UID, WORK_QUEUE.JSON)
          .from(WORK_QUEUE)
          .where(WORK_QUEUE.TYPE.eq(WorkQueueEventType.CODE.name))
          .orderBy(WORK_QUEUE.FIRST_SEEN)
          .limit(limit)
          .forUpdate()
          .fetch()
          .onEach { (uid, json) ->
            events.add(json)
            deleteFrom(WORK_QUEUE)
              .where(WORK_QUEUE.UID.eq(uid))
              .execute()
          }
      }

      events.mapNotNull { event ->
        try {
          mapper.readValue<CodeEvent>(event)
        } catch (e: JsonMappingException) {
          log.warn("Unable to parse queued code event, ignoring: {}", event)
          null
        }
      }
    }

  override fun addToQueue(artifactVersion: PublishedArtifact) {
    sqlRetry.withRetry(WRITE) {
      jooq.insertInto(WORK_QUEUE)
        .set(WORK_QUEUE.UID, randomUID().toString())
        .set(WORK_QUEUE.TYPE, WorkQueueEventType.ARTIFACT.name)
        .set(WORK_QUEUE.FIRST_SEEN, clock.instant())
        .set(WORK_QUEUE.JSON, mapper.writeValueAsString(artifactVersion))
        .execute()
    }
  }

  override fun removeArtifactsFromQueue(limit: Int): Collection<PublishedArtifact> =
    sqlRetry.withRetry(WRITE) {
      val artifacts = mutableListOf<String>()
      jooq.inTransaction {
        select(WORK_QUEUE.UID, WORK_QUEUE.JSON)
          .from(WORK_QUEUE)
          .where(WORK_QUEUE.TYPE.eq(WorkQueueEventType.ARTIFACT.name))
          .orderBy(WORK_QUEUE.FIRST_SEEN)
          .limit(limit)
          .forUpdate()
          .fetch()
          .onEach { (uid, json) ->
            artifacts.add(json)
            deleteFrom(WORK_QUEUE)
              .where(WORK_QUEUE.UID.eq(uid))
              .execute()
          }
      }
      artifacts.mapNotNull { artifact ->
        try {
          mapper.readValue<PublishedArtifact>(artifact)
        } catch (e: JsonMappingException) {
          log.warn("Unable to parse queued published artifact, ignoring: {}", artifact)
          null
        }
      }
    }

  override fun queueSize(): Int =
    sqlRetry.withRetry(RetryCategory.READ) {
      jooq.fetchCount(WORK_QUEUE)
    }
}
