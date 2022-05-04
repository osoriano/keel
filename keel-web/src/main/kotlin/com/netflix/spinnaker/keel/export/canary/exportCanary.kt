package com.netflix.spinnaker.keel.export.canary

import com.netflix.spinnaker.keel.front50.model.ChapCanaryStage
import com.netflix.spinnaker.keel.front50.model.Pipeline

/**
 * LIMITATIONS (things we are not handling because we don't want to build a whole expression evaluator here):
 * 1. configId might be parameterized, but only in developer canaries, so not worth resolving
 * 2. clusters might be parameterized, e.g.
 *    - aecom-prod-${parameters.cluster_description}
 *    - api-prod-x1${trigger.parameters.shard1}-x2${trigger.parameters.shard2}
 *    - cosmosplato-${parameters.stack}
 *    - ${parameters.cluster}
 *    - ${parameters["stackName"] != "" ? "playapi-"+parameters.stackName : "playapi"}
 *    - sessioncontext-prod-${trigger.parameters.shard}
 * 3. clusterTag might be parameterized
 *    - ${trigger.parameters.canaryName}
 *    - ${parameters.userName}
 *    - ${parameters.cluster_description}
 *    - ${parameters.Canary_Name}
 *    - ${#alphanumerical(parameters.Canary_Name)} (come on people)
 */

fun extractCanaryConstraints(pipeline: Pipeline): List<CanaryConstraint> {
  val toConvert: List<ChapCanaryStage> = pipeline.stages.filterIsInstance<ChapCanaryStage>()
  return toConvert.map { stage ->
    val parameters = stage.pipelineParameters
    val regions: List<ClusterRegion> = parameters.regionMetadata.map { region ->
      ClusterRegion(region = region.region, startDelayInMinutes = region.startDelayInMinutes)
    }
    CanaryConstraint(
      clusters = listOf(ClusterWithRegions(cluster = parameters.cluster, regions = regions, clusterTag = parameters.clusterTag)),
      configId = parameters.kayentaConfigIds[0],
      global = parameters.global,
      durationInMinutes = getNumericValue(parameters.durationInMinutes, pipeline, 120),
      resultStrategy = ResultStrategy.valueOf(parameters.constraints.resultStrategy.name),
      marginalScore = getNumericValue(parameters.constraints.canaryAnalysisThresholds.marginal, pipeline, 75),
      successfulScore = getNumericValue(parameters.constraints.canaryAnalysisThresholds.pass, pipeline, 95),
      intervalInMinutes = getNumericValue(parameters.intervalInMinutes, pipeline, 30),
      slidingWindow = parameters.slidingWindow,
      slidingWindowSize = parameters.slidingWindowSize,
      warmupInMinutes = getNumericValue(parameters.warmupInMinutes, pipeline, 0),
      numberOfInstances = getNumericValue(parameters.numberOfInstances, pipeline, null),
      baselinePropertyOverrides = parameters.baselinePropertyOverrides,
      configProperties = parameters.configProperties
    )
  }
}

fun getNumericValue(fieldVal: Any?, pipeline: Pipeline, defaultValue: Int?): Int? {
  var result = defaultValue
  if (fieldVal is Number) {
    result = fieldVal.toInt()
  }
  if (fieldVal is String) {
    try {
      result = fieldVal.toInt()
    } catch (_: NumberFormatException) {}
    // if the parameter is from the trigger, forget it, just use the default value; otherwise, try to parse
    if (fieldVal.contains("parameters") && !fieldVal.contains("trigger.parameters")) {
      // Parsing time
      val (paramName) = Regex("([a-zA-Z_]+)").find(fieldVal.substringAfter("parameters"))!!.destructured;
      val paramConfig = pipeline.parameterConfig.find { it.name == paramName }
      if (paramConfig?.default != null) {
        try {
          result = paramConfig.default!!.toInt()
          if (fieldVal.contains("*")) {
            // won't work on everything (e.g., other parameters), but will work on everything we are seeing
            // with canaries
            val (multiplier) = Regex("(\\d+)").find(fieldVal.substringAfter("*").trim())!!.destructured
            result *= multiplier.toInt()
          }
        } catch (_: NumberFormatException) {}
      }
    }
  }
  return result
}
