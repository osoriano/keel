package com.netflix.spinnaker.keel.export.canary

import com.netflix.spinnaker.keel.api.StatefulConstraint
import com.netflix.spinnaker.keel.api.schema.Title

// NOTE: This class is copied from
// https://stash.corp.netflix.com/projects/TRAFFIC/repos/chap-spinnaker/browse/chap-keel/src/main/kotlin/io/spinnaker/managed/CanaryConstraint.kt
// Make sure to keep these files in sync (though they should rarely change)
const val CURATED_CONFIG_ID: String = "12077d8c-8e4a-46ca-9e72-4b36f25d3287"

@Title("Canary")
data class CanaryConstraint(
    val clusters: List<ClusterWithRegions>? = null,
    val configId: String = CURATED_CONFIG_ID,
    val global: Boolean? = false,
    val durationInMinutes: Int? = null,
    val resultStrategy: ResultStrategy? = null,
    val marginalScore: Int? = null,
    val successfulScore: Int? = null,
    val intervalInMinutes: Int? = null,
    val slidingWindow: Boolean? = false,
    val slidingWindowSize: Int? = null,
    val warmupInMinutes: Int? = null,
    val numberOfInstances: Int? = null,
    val baselinePropertyOverrides: Map<String, String>? = null,
    val canaryPropertyOverrides: Map<String, String>? = null
) : StatefulConstraint("canary")

data class ClusterWithRegions(
    val cluster: String,
    val clusterTag: String? = null,
    val regions: List<ClusterRegion>? = null,
)

data class ClusterRegion(
    val region: String,
    val startDelayInMinutes: Int? = 0
)

data class ClusterWithRegionMetadata(
    val cluster: String? = null,
    val clusterTag: String? = null,
    val regionMetadata: List<RegionMetadata>? = null,
)

data class RegionMetadata(
    val region: String? = null,
    val startDelayInMinutes: Int? = 0,
    val imageId: String? = null,
    val numberOfInstances: Int? = null
)

data class EvaluationConstraints(
    val resultStrategy: ResultStrategy? = null,
    val canaryAnalysisThresholds: Thresholds? = null,
)

enum class ResultStrategy {
    LOWEST,
    AVERAGE
}

data class Thresholds(
    val pass: Int? = null,
    val marginal: Int? = null
)
