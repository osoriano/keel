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
  .withDatabaseName("keel")
  .withUsername("keel_service")
  .withPassword("whatever")
  .withReuse(true)
  .also { it.start() }

@Suppress("UsePropertyAccessSyntax")
private val JdbcDatabaseContainer<*>.authenticatedJdbcUrl: String
  get() = "${getJdbcUrl()}?user=${getUsername()}&password=${getPassword()}&useSSL=false"
