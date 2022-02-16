package com.netflix.spinnaker.keel.api.stash

data class Project(
  val key: String
)

data class Repo(
  val slug: String,
  val project: Project
)

data class Ref(
  val id: String,
  val repository: Repo
)
