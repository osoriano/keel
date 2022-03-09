package com.netflix.spinnaker.keel.jenkins

import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.spinnaker.keel.serialization.configuredObjectMapper
import org.junit.jupiter.api.Test

internal class DeserializationTests {
  private val jsonMapper = configuredObjectMapper()
  private val prettyPrinter = jsonMapper.writerWithDefaultPrettyPrinter()

  @Test
  fun `can parse Jenkins project XML`() {
    val xml = javaClass.getResource("/jenkins-job-with-tests.xml").readText()
    val parsed = jenkinsXmlMapper.readValue<JenkinsProject>(xml)
    println(prettyPrinter.writeValueAsString(parsed))
  }

  @Test
  fun `can parse Jenkins job`() {
    val encodedXml = javaClass.getResource("/jenkins-job-with-tests.xml").readText()
      .let { jsonMapper.writeValueAsString(it) }
    val json = """{"scmType": "ROCKET", "config": $encodedXml }"""
    val parsed = jsonMapper.readValue<Job>(json)
    println(prettyPrinter.writeValueAsString(parsed))
  }
}
