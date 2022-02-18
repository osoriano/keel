package com.netflix.spinnaker.keel.api

/**
 * This is a bridge to calling Igor for scm related endpoints.
 */
interface ScmBridge{

  suspend fun getScmInfo():
    Map<String, String?>
}
