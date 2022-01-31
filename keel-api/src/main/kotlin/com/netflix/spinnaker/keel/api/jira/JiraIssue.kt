package com.netflix.spinnaker.keel.api.jira

/*
Jira issue related objects
 */
data class JiraIssue(
  val fields: JiraFields,
  val transition: JiraTransition? = null
)

data class JiraTransition(
  val id: String?
)

data class JiraFields(
  val project: JiraProject = JiraProject(),
  val issuetype: JiraIssueType = JiraIssueType(),
  val assignee: JiraAssignee? = null,
  val components: List<JiraComponent>? = null,
  val summary: String,
  val description: String
)

data class JiraComponent(
  val name: String
)

data class JiraProject(
  val key: String? = "MD"
)

data class JiraIssueType(
  val name: String? = "Issue"
)

data class JiraAssignee(
  val name: String
)
