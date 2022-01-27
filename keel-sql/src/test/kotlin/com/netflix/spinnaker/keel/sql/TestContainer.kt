package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.kork.sql.test.SqlTestUtil.initDatabase
import org.jooq.SQLDialect.MYSQL
import org.jooq.conf.RenderMapping
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.MySQLContainerProvider
import org.testcontainers.jdbc.ConnectionUrl

internal val testDatabase by lazy {
  if (shouldUseLocalDatabase()) {
    initDatabase("jdbc:mysql://localhost/keel_sql_tests?user=root", MYSQL, "keel_sql_tests")
      .apply {
        // force JOOQ to output the correct schema
        context.settings().apply {
          withRenderSchema(false)
            .withRenderMapping((renderMapping ?: RenderMapping()).withDefaultSchema("keel_sql_tests"))
        }
      }
  } else {
    initDatabase(mySQLContainer.authenticatedJdbcUrl, MYSQL, "keel")
  }
}

private fun shouldUseLocalDatabase() =
  (System.getenv("SPRING_PROFILES_ACTIVE")?.split(",") ?: emptyList()).contains("localmysql")

internal val mySQLContainer by lazy {
  MySQLContainerProvider()
    .newInstance(ConnectionUrl.newInstance("jdbc:tc:mysql:8.0.27:///keel?useSSL=false&TC_REUSABLE=true"))
    .withCreateContainerCmdModifier { cmd ->
      cmd.hostConfig
        // According to the Docker docs, setting memory and memory swap to the same value prevents the container
        // from using swap at all: https://docs.docker.com/config/containers/resource_constraints/#prevent-a-container-from-using-swap
        ?.withMemory(512 * 1024 * 1024)
        ?.withMemorySwap(512 * 1024 * 1024)
        ?.withCpuCount(1)
    }
    .withDatabaseName("keel")
    .withUsername("keel_service")
    .withPassword("whatever")
    .withReuse(true)
    .also { it.start() }
}

@Suppress("UsePropertyAccessSyntax")
private val JdbcDatabaseContainer<*>.authenticatedJdbcUrl: String
  get() = "${getJdbcUrl()}?user=${getUsername()}&password=${getPassword()}&useSSL=false"
