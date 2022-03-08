package com.netflix.spinnaker.config

import org.springframework.context.annotation.Configuration
import org.springframework.core.Ordered.HIGHEST_PRECEDENCE
import org.springframework.core.annotation.Order
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter

/**
 * Disables several security-related filters for Slack callback requests, which are application/x-www-form-urlencoded
 * and cause the filters to read the request body while reading parameters, which in turn causes the input stream
 * to be closed before the request gets to the callback controller, appearing as an empty request body.
 */
@Configuration
@Order(1)
@EnableWebSecurity
class SlackRequestSecurityConfiguration : WebSecurityConfigurerAdapter() {

  override fun configure(http: HttpSecurity) {
    http
      .requestMatchers()
      // always authorize these URIs
      .antMatchers(
        "/slack/notifications/callbacks",
        "/poweruser/slack/message"
      )
      .and()
      .authorizeRequests()
      .anyRequest()
      .permitAll()
      .and()
      // disable CSRF checks for the same URIs
      .csrf()
      .ignoringAntMatchers(
        "/slack/notifications/callbacks",
        "/poweruser/slack/message"
      )
  }
}
