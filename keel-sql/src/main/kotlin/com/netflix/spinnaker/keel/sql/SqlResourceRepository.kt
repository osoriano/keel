package com.netflix.spinnaker.keel.sql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.convertValue
import com.netflix.kotlin.OpenClass
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.config.ResourceEventPruneConfig
import com.netflix.spinnaker.keel.api.Resource
import com.netflix.spinnaker.keel.api.ResourceKind.Companion.parseKind
import com.netflix.spinnaker.keel.api.ResourceSpec
import com.netflix.spinnaker.keel.api.ResourceStatus
import com.netflix.spinnaker.keel.api.ResourceStatusSnapshot
import com.netflix.spinnaker.keel.core.api.randomUID
import com.netflix.spinnaker.keel.events.ApplicationEvent
import com.netflix.spinnaker.keel.events.NonRepeatableEvent
import com.netflix.spinnaker.keel.events.PersistentEvent
import com.netflix.spinnaker.keel.events.PersistentEvent.EventScope
import com.netflix.spinnaker.keel.events.ResourceCheckResult
import com.netflix.spinnaker.keel.events.ResourceEvent
import com.netflix.spinnaker.keel.events.ResourceHistoryEvent
import com.netflix.spinnaker.keel.events.ResourceState
import com.netflix.spinnaker.keel.pause.PauseScope
import com.netflix.spinnaker.keel.persistence.NoSuchResourceId
import com.netflix.spinnaker.keel.persistence.ResourceHeader
import com.netflix.spinnaker.keel.persistence.ResourceRepository
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.ACTIVE_RESOURCE
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.DIFF_FINGERPRINT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.EVENT
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.PAUSED
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.RESOURCE
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.RESOURCE_LAST_CHECKED
import com.netflix.spinnaker.keel.persistence.metamodel.Tables.RESOURCE_VERSION
import com.netflix.spinnaker.keel.resources.ResourceFactory
import com.netflix.spinnaker.keel.sql.RetryCategory.READ
import com.netflix.spinnaker.keel.sql.RetryCategory.WRITE
import com.netflix.spinnaker.keel.telemetry.ResourceCheckCompleted
import de.huxhorn.sulky.ulid.ULID
import org.jooq.DSLContext
import org.jooq.Record1
import org.jooq.Select
import org.jooq.impl.DSL.coalesce
import org.jooq.impl.DSL.max
import org.jooq.impl.DSL.select
import org.jooq.impl.DSL.value
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.core.env.Environment
import java.time.Clock
import java.time.Duration
import java.time.Instant

@OpenClass
class SqlResourceRepository(
  private val jooq: DSLContext,
  override val clock: Clock,
  private val objectMapper: ObjectMapper,
  private val resourceFactory: ResourceFactory,
  private val sqlRetry: SqlRetry,
  private val spectator: Registry,
  private val springEnv: Environment,
  private val resourceEventPruneConfig: ResourceEventPruneConfig,
) : ResourceRepository {

  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  /**
   * Insert into RESOURCE_VERSION table is frequently deadlocking.
   * Creating a metric to get insight into affected apps
   */
  private val resourceVersionInsertId = spectator.createId("resource.version.insert")

  private val resourceFetchSize: Int
    get() = springEnv.getProperty("keel.resource-repository.batch-fetch-size", Int::class.java, 100)

  /**
   * Calls the specified [callback] for each resource in the database. Ignores temporary resources.
   */
  override fun allResources(callback: (ResourceHeader) -> Unit) {
    sqlRetry.withRetry(READ) {
      val cursor = jooq
        .select(RESOURCE.KIND, RESOURCE.ID, RESOURCE.UID, RESOURCE.APPLICATION)
        .from(RESOURCE)
        .where(RESOURCE.IS_DRYRUN.isFalse) // ignore dry-run resources
        .fetchSize(resourceFetchSize)
        .fetchLazy()

      while (cursor.hasNext()) {
        cursor.fetchNext(resourceFetchSize)
          .map { (kind, id, uid, application) ->
            ResourceHeader(id, parseKind(kind), uid, application)
          }
          .forEach(callback)
      }
    }
  }

  /**
   * @return An [Iterator] over all resources in the database. Ignores temporary resources.
   */
  override fun allResources(): Iterator<ResourceHeader> {
    return PagedIterator(
      sqlRetry,
      sqlRetry.withRetry(READ) {
        jooq
          .select(RESOURCE.KIND, RESOURCE.ID, RESOURCE.UID, RESOURCE.APPLICATION)
          .from(RESOURCE)
          .where(RESOURCE.IS_DRYRUN.isFalse) // ignore dry-run resources
          .fetchSize(resourceFetchSize)
          .fetchLazy()
      },
      resourceFetchSize
    ) { (kind, id, uid, application) ->
      ResourceHeader(id, parseKind(kind), uid, application)
    }
  }

  override fun count(): Int {
    return sqlRetry.withRetry(READ) {
      jooq.fetchCount(RESOURCE)
    }
  }

  override fun get(id: String): Resource<ResourceSpec> =
    readResource(id) { kind, metadata, spec ->
      resourceFactory.create(kind, metadata, spec, isDryRun = false)
    }

  override fun getRaw(id: String): Resource<ResourceSpec> =
    readResource(id) { kind, metadata, spec ->
      resourceFactory.createRaw(kind, metadata, spec, isDryRun = false)
    }

  private fun readResource(id: String, factoryCallback: ResourceFactoryCallback): Resource<ResourceSpec> =
    sqlRetry.withRetry(READ) {
      jooq
        .select(ACTIVE_RESOURCE.KIND, ACTIVE_RESOURCE.METADATA, ACTIVE_RESOURCE.SPEC, ACTIVE_RESOURCE.IS_DRYRUN)
        .from(ACTIVE_RESOURCE)
        .where(ACTIVE_RESOURCE.ID.eq(id))
        .and(ACTIVE_RESOURCE.IS_DRYRUN.isFalse) // ignore dry-run resources
        .fetch()
        .also {
          if (it.size > 1) {
            log.error("Duplicate active resource record for $id. This is bug! Please fix the cause.")
          }
        }
        .firstOrNull()
        ?.let { (kind, metadata, spec) ->
          factoryCallback(kind, metadata, spec)
        } ?: throw NoSuchResourceId(id)
    }

  override fun getResourcesByApplication(application: String): List<Resource<*>> {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ACTIVE_RESOURCE.KIND, ACTIVE_RESOURCE.METADATA, ACTIVE_RESOURCE.SPEC, ACTIVE_RESOURCE.IS_DRYRUN)
        .from(ACTIVE_RESOURCE)
        .where(ACTIVE_RESOURCE.APPLICATION.eq(application))
        .fetch()
        .map { (kind, metadata, spec, isDryRun) ->
          resourceFactory.create(kind, metadata, spec, isDryRun)
        }
    }
  }

  override fun getResourceIdsForClusterName(name: String): List<String> {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(ACTIVE_RESOURCE.ID)
        .from(ACTIVE_RESOURCE)
        .where(ACTIVE_RESOURCE.ID.like("%:cluster:%:$name"))
        .fetch(ACTIVE_RESOURCE.ID)
    }
  }

  override fun hasManagedResources(application: String): Boolean =
    sqlRetry.withRetry(READ) {
      jooq
        .selectCount()
        .from(RESOURCE)
        .where(RESOURCE.APPLICATION.eq(application))
        .and(RESOURCE.IS_DRYRUN.isFalse)
        .fetchSingle()
        .value1() > 0
    }

  override fun getResourceIdsByApplication(application: String): List<String> {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(RESOURCE.ID)
        .from(RESOURCE)
        .where(RESOURCE.APPLICATION.eq(application))
        .fetch(RESOURCE.ID)
    }
  }

  // todo: this is not retryable due to overall repository structure: https://github.com/spinnaker/keel/issues/740
  override fun <T : ResourceSpec> store(resource: Resource<T>): Resource<T> {
    val version = jooq.select(
        coalesce(
          max(RESOURCE_VERSION.VERSION),
          value(0)
        )
      )
      .from(RESOURCE_VERSION)
      .join(RESOURCE)
      .on(RESOURCE.UID.eq(RESOURCE_VERSION.RESOURCE_UID))
      .where(RESOURCE.ID.eq(resource.id))
      .fetchSingleInto<Int>()

    val uid = if (version > 0 ) {
      getResourceUid(resource.id)
    } else {
      randomUID().toString()
        .also { uid ->
          jooq.insertInto(RESOURCE)
            .set(RESOURCE.UID, uid)
            .set(RESOURCE.KIND, resource.kind.toString())
            .set(RESOURCE.ID, resource.id)
            .set(RESOURCE.APPLICATION, resource.application)
            .set(RESOURCE.IS_DRYRUN, resource.isDryRun)
            .execute()
        }
    }

    try {
      jooq.insertInto(RESOURCE_VERSION)
        .set(RESOURCE_VERSION.RESOURCE_UID, uid)
        .set(RESOURCE_VERSION.VERSION, version + 1)
        .set(RESOURCE_VERSION.METADATA, objectMapper.writeValueAsString(resource.metadata))
        .set(RESOURCE_VERSION.SPEC, objectMapper.writeValueAsString(resource.spec))
        .set(RESOURCE_VERSION.CREATED_AT, clock.instant())
        .execute()
      jooq.update(RESOURCE)
        .set(RESOURCE.KIND, resource.kind.toString())
        .where(RESOURCE.UID.eq(uid))
        .execute()
    } catch(e: Exception) {
      log.error("Failed to insert resource version for ${resource.id}: $e", e)
      spectator.counter(resourceVersionInsertId.withTags(
        "success", "false",
        "application", resource.application, // Capture the app on fail cases to help repro
        "kind", resource.kind.toString()
      ))
        .increment()
      throw e
    }

    spectator.counter(resourceVersionInsertId.withTag("success", "true")).increment()

    return resource.copy(
      metadata = resource.metadata + mapOf("uid" to uid, "version" to version + 1)
    )
  }

  override fun applicationEventHistory(application: String, limit: Int): List<ApplicationEvent> {
    require(limit > 0) { "limit must be a positive integer" }
    return sqlRetry.withRetry(READ) {
      jooq
        .select(EVENT.JSON)
        .from(EVENT)
        .where(EVENT.SCOPE.eq(EventScope.APPLICATION))
        .and(EVENT.REF.eq(application))
        .orderBy(EVENT.TIMESTAMP.desc())
        .limit(limit)
        .fetch(EVENT.JSON)
        .filterIsInstance<ApplicationEvent>()
    }
  }

  override fun applicationEventHistory(application: String, after: Instant): List<ApplicationEvent> {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(EVENT.JSON)
        .from(EVENT)
        .where(EVENT.SCOPE.eq(EventScope.APPLICATION))
        .and(EVENT.REF.eq(application))
        .and(EVENT.TIMESTAMP.greaterOrEqual(after))
        .orderBy(EVENT.TIMESTAMP.desc())
        .fetch(EVENT.JSON)
        .filterIsInstance<ApplicationEvent>()
    }
  }

  override fun eventHistory(id: String, limit: Int): List<ResourceHistoryEvent> {
    require(limit > 0) { "limit must be a positive integer" }

    return sqlRetry.withRetry(READ) {
      jooq
        .select(EVENT.JSON)
        .from(EVENT)
        // look for resource events that match the resource...
        .where(
          EVENT.SCOPE.eq(EventScope.RESOURCE)
            .and(EVENT.REF.eq(id))
        )
        // ...or application events that match the application as they apply to all resources
        .or(
          EVENT.SCOPE.eq(EventScope.APPLICATION)
            .and(EVENT.APPLICATION.eq(applicationForId(id)))
        )
        .orderBy(EVENT.TIMESTAMP.desc())
        .limit(limit)
        .fetch(EVENT.JSON)
        // filter out application events that don't affect resource history
        .filterIsInstance<ResourceHistoryEvent>()
    }
  }

  // todo: add sql retries once we've rethought repository structure: https://github.com/spinnaker/keel/issues/740
  override fun appendHistory(event: ResourceEvent) {
    doAppendHistory(event)
  }

  override fun appendHistory(event: ApplicationEvent) {
    doAppendHistory(event)
  }

  private fun doAppendHistory(event: PersistentEvent) {
    log.debug("Appending event: $event")

    if (event is NonRepeatableEvent) {
      val previousEventWithUid = sqlRetry.withRetry(READ) {
        jooq
          .select(EVENT.UID, EVENT.JSON)
          .from(EVENT)
          // look for resource events that match the resource...
          .where(
            EVENT.SCOPE.eq(EventScope.RESOURCE)
              .and(EVENT.REF.eq(event.ref))
          )
          // ...or application events that match the application as they apply to all resources
          .or(
            EVENT.SCOPE.eq(EventScope.APPLICATION)
              .and(EVENT.APPLICATION.eq(event.application))
          )
          .orderBy(EVENT.TIMESTAMP.desc())
          .limit(1)
          .fetchOne()
      }

      val previousEventUid = previousEventWithUid?.value1()
      val previousEvent = previousEventWithUid?.value2()

      if (event.javaClass == previousEvent?.javaClass) {
        // if a previous event exists, we update the timestamp and the count in the JSON
        // TODO: the multiple conversions to/from JSON here are wasteful, but currently the table
        //  is structured such that most columns are generated from the JSON.
        val updatedEvent = objectMapper.convertValue<MutableMap<String, Any?>>(previousEvent)
          .let { eventMap ->
            eventMap["timestamp"] = event.timestamp
            eventMap["firstTriggeredAt"] = previousEvent.timestamp
            eventMap["count"] = (previousEvent as NonRepeatableEvent).count + 1
            objectMapper.convertValue<PersistentEvent>(eventMap)
          }
        sqlRetry.withRetry(WRITE) {
          jooq
            .update(EVENT)
            .set(EVENT.JSON, updatedEvent)
            .where(EVENT.UID.eq(previousEventUid))
            .execute()
        }
        return
      }
    }

    sqlRetry.withRetry(WRITE) {
      jooq
        .insertInto(EVENT)
        .set(EVENT.UID, ULID().nextULID(event.timestamp.toEpochMilli()))
        .set(EVENT.SCOPE, event.scope)
        .set(EVENT.JSON, event)
        .execute()
    }
  }

  override fun updateStatus(resourceId: String, status: ResourceStatus) {
    sqlRetry.withRetry(WRITE) {
      jooq
        .update(RESOURCE)
        .set(RESOURCE.STATUS, status)
        .set(RESOURCE.STATUS_UPDATED_AT, clock.instant())
        .where(RESOURCE.ID.eq(resourceId))
        .execute()
    }
  }

  override fun getStatus(resourceId: String): ResourceStatusSnapshot? {
    return sqlRetry.withRetry(READ) {
      jooq
        .select(RESOURCE.STATUS, RESOURCE.STATUS_UPDATED_AT)
        .from(RESOURCE)
        .where(RESOURCE.ID.eq(resourceId))
        .and(RESOURCE.STATUS.isNotNull)
        .and(RESOURCE.STATUS_UPDATED_AT.isNotNull)
        .fetchOne { (status, updatedAt) ->
          ResourceStatusSnapshot(status, updatedAt)
        }
    }
  }

  override fun delete(id: String) {
    // TODO: these should be run inside a transaction
    sqlRetry.withRetry(WRITE) {
      jooq.deleteFrom(RESOURCE)
        .where(RESOURCE.ID.eq(id))
        .execute()
        .also { count ->
          if (count == 0) {
            throw NoSuchResourceId(id)
          }
        }
    }
    sqlRetry.withRetry(WRITE) {
      jooq.deleteFrom(EVENT)
        .where(EVENT.SCOPE.eq(EventScope.RESOURCE))
        .and(EVENT.REF.eq(id))
        .execute()
    }
    sqlRetry.withRetry(WRITE) {
      jooq.deleteFrom(DIFF_FINGERPRINT)
        .where(DIFF_FINGERPRINT.ENTITY_ID.eq(id))
        .execute()
    }
    sqlRetry.withRetry(WRITE) {
      jooq.deleteFrom(PAUSED)
        .where(PAUSED.SCOPE.eq(PauseScope.RESOURCE))
        .and(PAUSED.NAME.eq(id))
        .execute()
    }
  }

  override fun getLastCheckedTime(resource: Resource<*>): Instant? =
    sqlRetry.withRetry(READ) {
      jooq
        .select(RESOURCE_LAST_CHECKED.AT)
        .from(RESOURCE_LAST_CHECKED)
        .where(RESOURCE_LAST_CHECKED.RESOURCE_UID.eq(resource.uid))
        .fetchOne(RESOURCE_LAST_CHECKED.AT)
    }

  override fun setLastCheckedTime(resource: Resource<*>) {
    sqlRetry.withRetry(WRITE) {
      val now = clock.instant()
      jooq
        .insertInto(RESOURCE_LAST_CHECKED)
        .set(RESOURCE_LAST_CHECKED.RESOURCE_UID, resource.uid)
        .set(RESOURCE_LAST_CHECKED.AT, now)
        .onDuplicateKeyUpdate()
        .set(RESOURCE_LAST_CHECKED.AT, now)
        .execute()
    }
  }

  fun markCheckComplete(resource: Resource<*>, status: Any?) {
    require(status is ResourceState)
    sqlRetry.withRetry(WRITE) {
      val now = clock.instant()
      jooq
        .insertInto(RESOURCE_LAST_CHECKED)
        .set(RESOURCE_LAST_CHECKED.RESOURCE_UID, resource.uid)
        .set(RESOURCE_LAST_CHECKED.STATUS, status)
        .set(RESOURCE_LAST_CHECKED.STATUS_DETERMINED_AT, now)
        .onDuplicateKeyUpdate()
        .set(RESOURCE_LAST_CHECKED.STATUS, status)
        .set(RESOURCE_LAST_CHECKED.STATUS_DETERMINED_AT, now)
        .execute()
    }
  }

  @EventListener(ResourceCheckResult::class)
  fun onResourceChecked(event: ResourceCheckResult) {
    markCheckComplete(get(event.id), event.state)
  }

  override fun incrementDeletionAttempts(resource: Resource<*>) {
    sqlRetry.withRetry(WRITE) {
      jooq.update(RESOURCE)
        .set(RESOURCE.ATTEMPTED_DELETIONS, RESOURCE.ATTEMPTED_DELETIONS + 1)
        .where(RESOURCE.ID.eq(resource.id))
        .execute()
    }
  }

  override fun countDeletionAttempts(resource: Resource<*>): Int {
    return sqlRetry.withRetry(READ) {
      jooq.select(RESOURCE.ATTEMPTED_DELETIONS)
        .from(RESOURCE)
        .where(RESOURCE.ID.eq(resource.id))
        .fetchOne(RESOURCE.ATTEMPTED_DELETIONS)!!
    }
  }

  /**
   * Prunes some history once a resource check is completed so that
   * the history does not grow without bound.
   */
  @EventListener(ResourceCheckCompleted::class)
  fun pruneHistory(event: ResourceCheckCompleted) {
    val count = sqlRetry.withRetry(READ){
      jooq
        .selectCount()
        .from(EVENT)
        .where(EVENT.SCOPE.eq(EventScope.RESOURCE))
        .and(EVENT.REF.eq(event.resourceID))
        .fetchSingleInto<Int>()
    }
    if (count <= resourceEventPruneConfig.minEventsKept) {
      // leave the latest minEventsKept rows.
      return
    }
    val deleteChunkSize = if (count - resourceEventPruneConfig.deleteChunkSize < resourceEventPruneConfig.minEventsKept) {
      // if we're close to the min events kept, change how many to delete
      count - resourceEventPruneConfig.minEventsKept
    } else {
      resourceEventPruneConfig.deleteChunkSize
    }

    // delete things older than 14 days
    val recordsDeleted: Int = sqlRetry.withRetry(WRITE) {
      jooq.deleteFrom(EVENT)
          .where(EVENT.SCOPE.eq(EventScope.RESOURCE))
          .and(EVENT.REF.eq(event.resourceID))
          .and(EVENT.TIMESTAMP.lessOrEqual(clock.instant().minus(Duration.ofDays(resourceEventPruneConfig.daysKept))))
          .orderBy(EVENT.TIMESTAMP) // order so we delete the oldest records first
          .limit(deleteChunkSize) // delete in chunks so that we always have at least resourceEventPruneConfig.minEventsKept records
          .execute()
      }
    log.debug("Deleted $recordsDeleted for ${event.resourceID} older than ${resourceEventPruneConfig.daysKept} days")
  }

  fun getResourceUid(id: String) =
    sqlRetry.withRetry(READ) {
      jooq
        .select(RESOURCE.UID)
        .from(RESOURCE)
        .where(RESOURCE.ID.eq(id))
        .fetchOne(RESOURCE.UID)
        ?: throw IllegalStateException("Resource with id $id not found. Retrying.")
    }

  private fun applicationForId(id: String): Select<Record1<String>> =
    select(RESOURCE.APPLICATION)
      .from(RESOURCE)
      .where(RESOURCE.ID.eq(id))
      .limit(1)

  private val Resource<*>.uid: String
    get() = getResourceUid(id)

  private fun String.unquote() =
    if (startsWith('"') && endsWith('"')) this.substring(1, length - 1) else this
}

typealias ResourceFactoryCallback = (kind: String, metadata: String, spec: String) -> Resource<ResourceSpec>
