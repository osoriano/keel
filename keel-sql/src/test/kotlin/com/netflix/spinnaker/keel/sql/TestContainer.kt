package com.netflix.spinnaker.keel.sql

import com.netflix.spinnaker.kork.sql.test.SqlTestUtil.initDatabase
import org.jooq.SQLDialect.MYSQL
import org.testcontainers.containers.JdbcDatabaseContainer
import org.testcontainers.containers.MySQLContainerProvider
import org.testcontainers.jdbc.ConnectionUrl

internal val testDatabase by lazy {
  initDatabase(mySQLContainer.authenticatedJdbcUrl, MYSQL, "keel")
}

internal val mySQLContainer = MySQLContainerProvider()
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

@Suppress("UsePropertyAccessSyntax")
private val JdbcDatabaseContainer<*>.authenticatedJdbcUrl: String
  get() = "${getJdbcUrl()}?user=${getUsername()}&password=${getPassword()}&useSSL=false"
