package com.netflix.spinnaker.config

import org.springframework.core.Ordered.HIGHEST_PRECEDENCE
import org.springframework.core.annotation.Order
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter

@EnableWebSecurity
@Order(HIGHEST_PRECEDENCE)
class WebSecurityConfiguration : WebSecurityConfigurerAdapter() {

  /**
   * Allow embedding of our reports in iframes.
   */
  override fun configure(http: HttpSecurity) {
    http
      .antMatcher("/reports/**")
      .headers()
      .frameOptions()
      .disable()
  }
}
