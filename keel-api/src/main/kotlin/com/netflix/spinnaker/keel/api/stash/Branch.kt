package com.netflix.spinnaker.keel.api.stash


/** A source control branch. */
data class Branch(
  val name: String,
  val ref: String,
  val default: Boolean = false
)

/** A source control branch as represented in stash. */
data class BranchResponse(
  //the ref of the branch
  val id: String,
  val displayId: String,
)
