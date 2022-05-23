package com.netflix.spinnaker.keel.config

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(WorkProcessingConfig::class)
class ArtifactConfiguration
