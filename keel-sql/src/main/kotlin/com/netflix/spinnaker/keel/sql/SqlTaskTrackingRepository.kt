package com.netflix.spinnaker.keel.sql

import com.netflix.spectator.api.BasicTag
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.RetentionProperties
import com.netflix.spinnaker.keel.api.TaskStatus
import com.netflix.spinnaker.keel.api.TaskStatus.RUNNING
import com.netflix.spinnaker.keel.persistence.TaskRecord
import com.netflix.spinnaker.keel.persistence.TaskTrackingRepository
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.TASK_TRACKING
import com.netflix.spinnaker.keel.sql.RetryCategory.READ
import com.netflix.spinnaker.keel.sql.RetryCategory.WRITE
import org.jooq.DSLContext
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

class SqlTaskTrackingRepository(
  private val jooq: DSLContext,
  private val clock: Clock,
  private val sqlRetry: SqlRetry,
  private val publisher: ApplicationEventPublisher,
  private val spectator: Registry,
) : TaskTrackingRepository {

  override fun store(task: TaskRecord) {
    sqlRetry.withRetry(WRITE) {
      jooq.insertInto(TASK_TRACKING)
        .set(TASK_TRACKING.SUBJECT_TYPE, task.subjectType)
        .set(TASK_TRACKING.TASK_ID, task.id)
        .set(TASK_TRACKING.TASK_NAME, task.name)
        .set(TASK_TRACKING.STARTED_AT, clock.instant())
        .set(TASK_TRACKING.STATUS, RUNNING)
        .set(TASK_TRACKING.APPLICATION, task.application)
        .set(TASK_TRACKING.ENVIRONMENT_NAME, task.environmentName)
        .set(TASK_TRACKING.RESOURCE_ID, task.resourceId)
        .onDuplicateKeyIgnore()
        .execute()
    }
  }

  override fun getIncompleteTasks(minTimeSinceLastCheck: Duration, limit: Int): Set<TaskRecord> {
    val now = clock.instant()
    val cutoff = now.minus(minTimeSinceLastCheck)
    return sqlRetry.withRetry(WRITE) {
      jooq.inTransaction {
        select(
          TASK_TRACKING.TASK_ID,
          TASK_TRACKING.TASK_NAME,
          TASK_TRACKING.SUBJECT_TYPE,
          TASK_TRACKING.APPLICATION,
          TASK_TRACKING.ENVIRONMENT_NAME,
          TASK_TRACKING.RESOURCE_ID,
          TASK_TRACKING.LAST_CHECKED
        )
        .from(TASK_TRACKING)
        .where(TASK_TRACKING.LAST_CHECKED.lessOrEqual(cutoff))
        .orderBy(TASK_TRACKING.LAST_CHECKED)
        .limit(limit)
        .forUpdate()
        .fetch()
        .also {
          var maxLagTime = Duration.ZERO
          val now = clock.instant()
          it.forEach { (taskId, _, _, _, _, _, lastCheckedAt ) ->
            update(TASK_TRACKING)
              .set(TASK_TRACKING.LAST_CHECKED, now)
              .where(TASK_TRACKING.TASK_ID.eq(taskId))
              .execute()
            if (lastCheckedAt != null && lastCheckedAt.isAfter(Instant.EPOCH.plusSeconds(2))) {
              val lagTime = Duration.between(lastCheckedAt, now)
              if (maxLagTime.compareTo(lagTime) < 0) {
                maxLagTime = lagTime
              }
            }
          }
          spectator.timer(
            "keel.scheduled.method.max.lag",
            listOf(
              BasicTag("type", "task")
            )
          ).record(maxLagTime.toSeconds(), TimeUnit.SECONDS)
        }
      }
      .map { (taskId, taskName, subjectType, application, environmentName, resourceId, _) ->
        TaskRecord(taskId, taskName, subjectType, application, environmentName, resourceId)
      }
      .toSet()
    }
  }

  override fun delete(taskId: String) {
    sqlRetry.withRetry(WRITE) {
      jooq.deleteFrom(TASK_TRACKING)
        .where(TASK_TRACKING.TASK_ID.eq(taskId))
        .execute()
    }
  }

  private val log by lazy { LoggerFactory.getLogger(javaClass) }
}
