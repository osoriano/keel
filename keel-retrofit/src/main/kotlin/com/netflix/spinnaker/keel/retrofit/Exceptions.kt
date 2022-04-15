package com.netflix.spinnaker.keel.retrofit

import org.springframework.http.HttpStatus.UNAUTHORIZED
import org.springframework.http.HttpStatus.NOT_FOUND
import retrofit2.HttpException

/**
 * Is this exception an HTTP 404?
 */
val Throwable.isNotFound: Boolean
  get() = this is HttpException && code() == NOT_FOUND.value()


/**
 * Is this exception an HTTP 401?
 */
val Throwable.isUnauthorized: Boolean
get() = this is HttpException && code() == UNAUTHORIZED.value()
