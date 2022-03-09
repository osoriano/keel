package com.netflix.spinnaker.keel.jenkins

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer
import com.fasterxml.jackson.dataformat.xml.XmlMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.netflix.spinnaker.keel.jenkins.PublisherType.HTML_REPORT
import com.netflix.spinnaker.keel.jenkins.PublisherType.JUNIT_REPORT

internal val jenkinsXmlMapper = XmlMapper()
  .registerKotlinModule()
  .disable(FAIL_ON_UNKNOWN_PROPERTIES)

private val buildersListType =
  jenkinsXmlMapper.typeFactory.constructCollectionType(List::class.java, BuilderConfig::class.java)

private val publishersListType =
  jenkinsXmlMapper.typeFactory.constructCollectionType(List::class.java, PublisherConfig::class.java)

data class Job(
  val config: JobConfig,
  val scmType: ScmType
)

enum class JobState {
  DISABLED, ACTIVE, DEFUNCT
}

enum class ScmType {
  ROCKET, LEGACY, NONE, UNKNOWN
}

data class JobConfig(
  val project: JenkinsProject
) {
  @JsonCreator
  constructor(projectXml: String) : this(jenkinsXmlMapper.readValue<JenkinsProject>(projectXml))
}

data class JenkinsProject(
  val description: String = "N/A",
  val disabled: Boolean = false,
  val assignedNode: String? = null,
  val jdk: String? = null,
  val scm: ScmConfig? = null,
  @JsonDeserialize(using = BuilderConfigDeserializer::class)
  val builders: List<BuilderConfig> = emptyList(),
  @JsonDeserialize(using = PublisherConfigDeserializer::class)
  val publishers: List<PublisherConfig> = emptyList()
)

data class ParameterConfig(
  val values: List<String>,
  val default: String? = null
)

data class ScmConfig(
  @JsonAlias("class")
  val className: String
)

val BUILDER_TYPES = mapOf(
  "hudson.plugins.gradle.Gradle" to GradleConfig::class
)

@JsonDeserialize(using = BuilderConfigDeserializer::class)
interface BuilderConfig

@JsonDeserialize(using = JsonDeserializer.None::class)
data class GradleConfig(
  val switches: String,
  val tasks: String,
  val rootBuildScriptDir: String,
  val buildFile: String,
  val useWrapper: Boolean,
  val wrapperScript: String
) : BuilderConfig

val PUBLISHER_TYPES = mapOf(
  "htmlpublisher.HtmlPublisher" to HtmlReportConfig::class,
  "hudson.tasks.junit.JUnitResultArchiver" to JUnitReportConfig::class
)

enum class PublisherType { HTML_REPORT, JUNIT_REPORT, EMAIL }

interface PublisherConfig {
  val type: PublisherType
  val reports: List<BasicReport>
}

@JsonDeserialize(using = JsonDeserializer.None::class)
data class HtmlReportConfig(
  override val reports: List<BasicReport>
) : PublisherConfig {
  override val type = HTML_REPORT
}

data class BasicReport(
  val reportName: String,
  val reportDir: String,
  val reportFiles: String
)

@JsonDeserialize(using = JsonDeserializer.None::class)
data class JUnitReportConfig(
  override val reports: List<BasicReport>
) : PublisherConfig {
  override val type = JUNIT_REPORT
}

class BuilderConfigDeserializer : StdNodeBasedDeserializer<List<BuilderConfig>>(buildersListType) {
  override fun convert(root: JsonNode, ctx: DeserializationContext): List<BuilderConfig> {
    return root.fields().mapNotNull { (name, props) ->
      val type = BUILDER_TYPES[name] ?: return@mapNotNull null
      jenkinsXmlMapper.convertValue(props, type.java)
    }
  }
}

class PublisherConfigDeserializer : StdNodeBasedDeserializer<List<PublisherConfig>>(publishersListType) {
  override fun convert(root: JsonNode, ctx: DeserializationContext): List<PublisherConfig> {
    return root.fields().mapNotNull{ (name, props) ->
      val type = PUBLISHER_TYPES[name] ?: return@mapNotNull null
      when (type) {
        HtmlReportConfig::class -> {
          val reports = props.path("reportTargets").map { reportTarget ->
            jenkinsXmlMapper.convertValue(reportTarget, BasicReport::class.java)
          }
          HtmlReportConfig(reports)
        }
        JUnitReportConfig::class -> {
          val reports = listOf(
            BasicReport("JUnit", ".", props.get("testResults").asText())
          )
          JUnitReportConfig(reports)
        }
        else -> null
      }
    }
  }
}

internal fun  <T, R : Any> Iterator<T>.mapNotNull(transform: (T) -> R?) =
  asSequence().toList().mapNotNull(transform)
