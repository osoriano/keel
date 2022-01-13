package com.netflix.spinnaker.keel.testcontainers

import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.ImageNameSubstitutor

class MySqlImageNameSubstitutor : ImageNameSubstitutor() {
  override fun apply(original: DockerImageName): DockerImageName =
    if (System.getProperty("os.arch") == "aarch64" && original.isCompatibleWith(DockerImageName.parse("mysql"))) {
      DockerImageName.parse("jamielsharief/mysql:arm64")
    } else {
      original
    }

  override fun getDescription(): String = "Substitutes ARM native MySQL image for M1 Macs"
}
