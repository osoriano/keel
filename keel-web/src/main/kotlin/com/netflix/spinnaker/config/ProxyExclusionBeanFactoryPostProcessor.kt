package com.netflix.spinnaker.config

import org.slf4j.LoggerFactory
import org.springframework.aop.aspectj.annotation.AnnotationAwareAspectJAutoProxyCreator
import org.springframework.aop.config.AopConfigUtils.AUTO_PROXY_CREATOR_BEAN_NAME
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.EnvironmentAware
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

@ConfigurationProperties
class BeanExclusionConfiguration {
  var beanNames: List<String> = emptyList()
  var beanClasses: List<String> = emptyList()
}

/**
 * An auto proxy creator that extends [AnnotationAwareAspectJAutoProxyCreator] by excluding beans
 * from proxying based on configuration.
 */
class ExclusionAwareAutoProxyCreator(
  var exclusionConfig: BeanExclusionConfiguration
) : AnnotationAwareAspectJAutoProxyCreator() {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  override fun shouldSkip(beanClass: Class<*>, beanName: String): Boolean {
    return (beanName in exclusionConfig.beanNames).also { excluded ->
      if (excluded) {
        log.debug("Disabling proxying for bean '$beanName' in exclusion list")
      }
    }
  }
}

/**
 * A [BeanFactoryPostProcessor] that configures an [ExclusionAwareAutoProxyCreator] to prevent
 * proxying of beans in a configurable exclusion list.
 */
@Configuration
@EnableConfigurationProperties(BeanExclusionConfiguration::class)
class ProxyExclusionBeanFactoryPostProcessor : BeanFactoryPostProcessor, EnvironmentAware {
  private val log by lazy { LoggerFactory.getLogger(javaClass) }

  private lateinit var environment: Environment

  /**
   * Replaces the internal auto proxy creator bean with [ExclusionAwareAutoProxyCreator] so that we can force
   * exclusion of certain beans from proxying.
   *
   * Based on an idea shared here: https://stackoverflow.com/a/16032787
   *
   * @param beanFactory the bean factory used by the application context
   * @throws org.springframework.beans.BeansException in case of errors
   */
  override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
    val autoProxyCreator = beanFactory.getBeanDefinition(AUTO_PROXY_CREATOR_BEAN_NAME)
    log.debug("Found auto proxy creator: $autoProxyCreator")
    if (autoProxyCreator.beanClassName == AnnotationAwareAspectJAutoProxyCreator::class.java.name) {
      (beanFactory as? DefaultListableBeanFactory)?.run {
        log.debug("Replacing original auto proxy creator with ${ExclusionAwareAutoProxyCreator::class.qualifiedName}")
        autoProxyCreator.beanClassName = ExclusionAwareAutoProxyCreator::class.java.name
        val exclusionConfig = Binder.get(environment)
          .bind("spring.auto-proxy.exclude", BeanExclusionConfiguration::class.java)
          .orElse(BeanExclusionConfiguration())
        autoProxyCreator.propertyValues.add("exclusionConfig", exclusionConfig)
      }
    }
  }

  /**
   * Set the `Environment` that this component runs in.
   */
  override fun setEnvironment(environment: Environment) {
    this.environment = environment
  }
}
