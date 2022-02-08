package com.netflix.spinnaker.keel.jira

import com.netflix.spinnaker.keel.api.JiraBridge
import com.netflix.spinnaker.keel.api.jira.JiraComment
import com.netflix.spinnaker.keel.api.jira.JiraIssue
import com.netflix.spinnaker.keel.api.jira.JiraIssueResponse
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path

/**
 * Jira interface to create and update issues.
 * Documentation: https://docs.atlassian.com/software/jira/docs/api/REST/8.13.15/
 */
interface JiraService: JiraBridge {
  @GET("/rest/api/2/issue/{issueId}/transitions")
  override suspend fun getTransitionsByIssue(
    @Path("issueId") issueId: String,
  ): Map<String, Any?>

  @POST("/rest/api/2/issue")
  override suspend fun createIssue(
    @Body jiraIssue: JiraIssue
  ): JiraIssueResponse

  @POST("/rest/api/2/issue/{issueId}/comment")
  override suspend fun addComment(
    @Path("issueId") issueId: String,
    @Body jiraComment: JiraComment
  ): Map<String, Any?>
}
