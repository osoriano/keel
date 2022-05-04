package com.netflix.spinnaker.keel.export

import com.netflix.spinnaker.keel.export.canary.getNumericValue
import com.netflix.spinnaker.keel.front50.model.*
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo

internal class CanaryConstraintExportTests {

  private val pipeline: Pipeline = Pipeline(
    name = "nice pipeline",
    id = "a",
    application = "keel",
    parameterConfig = listOf(
      PipelineParameterConfig(name = "withNumberDefault", default = "60"),
      PipelineParameterConfig(name = "withStringDefault", default = "abc"),
      PipelineParameterConfig(name = "withoutDefault")
    ),
    _stages = emptyList()
  )

  @Test
  fun `getNumber handles what it needs to handle`() {
    // can we handle numbers and strings?
    expectThat(getNumericValue(100, pipeline, 50)).isEqualTo(100)
    expectThat(getNumericValue("100", pipeline, 50)).isEqualTo(100)

    // can we correctly extract a default value?
    expectThat(getNumericValue("\${parameters.withNumberDefault}", pipeline, 50)).isEqualTo(60)

    // do we ignore trigger parameters?
    expectThat(getNumericValue("\${trigger.parameters.withNumberDefault}", pipeline, 50)).isEqualTo(50)

    // do we ignore string or no-default parameters?
    expectThat(getNumericValue("\${parameters.withStringDefault}", pipeline, 50)).isEqualTo(50)
    expectThat(getNumericValue("\${parameters.withoutDefault}", pipeline, 50)).isEqualTo(50)

    // what about parameters that don't exist?
    expectThat(getNumericValue("\${parameters.nonExistent}", pipeline, 50)).isEqualTo(50)

    // can we do multiplication?
    expectThat(getNumericValue("\${#toInt(parameters.withNumberDefault)*60}", pipeline, 50)).isEqualTo(3600)
    expectThat(getNumericValue("\${ #toInt( parameters.withNumberDefault ) * 10 }", pipeline, 50)).isEqualTo(600)

    // do we skip multiplication when we have to fall back to the default value?
    expectThat(getNumericValue("\${ #toInt( parameters.withStringDefault ) * 10 }", pipeline, 50)).isEqualTo(50)
  }
}
