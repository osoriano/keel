package com.netflix.spinnaker.keel.exceptions

import com.netflix.spinnaker.kork.exceptions.SystemException

class ArtifactNotSupportedException(message: String) : SystemException(message)
