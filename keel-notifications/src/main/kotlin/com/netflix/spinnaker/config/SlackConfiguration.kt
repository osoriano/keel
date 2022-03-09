package com.netflix.spinnaker.config

import com.slack.api.bolt.App
import com.slack.api.bolt.AppConfig
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@ConditionalOnProperty("slack.enabled")
@ConfigurationProperties(prefix = "slack")
class SlackConfiguration {
  var token: String = "configure-me"
  var appToken: String = "configure-me"
  var defaultEmailDomain: String = "configure-me"
  var socketMode: Boolean = true
}

@Configuration
class SlackBotConfiguration {

  @Bean
  fun appConfig(config: SlackConfiguration): AppConfig {
    return AppConfig.builder()
      .singleTeamBotToken(config.token)
      .scope("commands,chat:write")
      .classicAppPermissionsEnabled(true)
      .build()
  }

  @Bean
  fun slackApp(appConfig: AppConfig): App {
    return App(appConfig)
  }
}
