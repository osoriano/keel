package com.netflix.spinnaker.keel.config

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import de.danielbechler.util.Assert
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(WorkProcessingConfig::class)
class SqsConfiguration {

  @Bean(destroyMethod = "shutdown")
  fun amazonSqsClient(
    awsCredentialsProvider: AWSCredentialsProvider,
    workProcessingConfig: WorkProcessingConfig
  ): AmazonSQS {
    Assert.notNull(workProcessingConfig.artifactSqsQueueUrl, "artifactSqsQueueUrl must be set")
    Assert.notNull(workProcessingConfig.codeEventSqsQueueUrl, "codeEventSqsQueueUrl must be set")

    return AmazonSQSClientBuilder.standard()
      .withCredentials(awsCredentialsProvider)
      .withClientConfiguration(ClientConfiguration())
      .withRegion("us-west-2")
      .build()
  }
}
