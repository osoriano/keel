package com.netflix.spinnaker.keel.rest

import com.netflix.spinnaker.fiat.shared.FiatPermissionEvaluator
import com.netflix.spinnaker.keel.api.DeliveryConfig
import com.netflix.spinnaker.keel.api.Environment
import com.netflix.spinnaker.keel.auth.PermissionLevel.READ
import com.netflix.spinnaker.keel.auth.PermissionLevel.WRITE
import com.netflix.spinnaker.keel.auth.AuthorizationSupport
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.APPLICATION
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.DELIVERY_CONFIG
import com.netflix.spinnaker.keel.auth.AuthorizationSupport.TargetEntity.RESOURCE
import com.netflix.spinnaker.keel.persistence.KeelRepository
import com.netflix.spinnaker.keel.test.computeResource
import com.netflix.spinnaker.keel.test.resource
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import org.springframework.security.access.AccessDeniedException
import org.springframework.security.core.context.SecurityContextHolder
import strikt.api.DescribeableBuilder
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isSuccess
import strikt.assertions.isTrue

internal class AuthorizationSupportTests : JUnit5Minutests {
  private val dynamicConfigService: DynamicConfigService = mockk()
  private val permissionEvaluator: FiatPermissionEvaluator = mockk()
  private val combinedRepository: KeelRepository = mockk()
  private val locatableResource = computeResource()
  private val nonLocatableResource = resource()
  private val deliveryConfig = DeliveryConfig(
    name = "manifest",
    application = ApplicationControllerTests.application,
    serviceAccount = "keel@spinnaker",
    artifacts = emptySet(),
    environments = setOf(Environment("test", setOf(locatableResource, nonLocatableResource)))
  )
  private val application = deliveryConfig.application

  fun tests() = rootContext<AuthorizationSupport> {
    fixture {
      AuthorizationSupport(permissionEvaluator, combinedRepository, dynamicConfigService)
    }

    context("authorization is enabled") {
      before {
        mockkStatic("org.springframework.security.core.context.SecurityContextHolder")

        every {
          SecurityContextHolder.getContext()
        } returns mockk(relaxed = true)

        every {
          dynamicConfigService.isEnabled("keel.authorization", true)
        } returns true

        every {
          combinedRepository.getResource(locatableResource.id)
        } returns locatableResource

        every {
          combinedRepository.getResource(nonLocatableResource.id)
        } returns nonLocatableResource

        every {
          combinedRepository.getDeliveryConfig(deliveryConfig.name)
        } returns deliveryConfig

        every {
          combinedRepository.getDeliveryConfigForApplication(application)
        } returns deliveryConfig
      }

      mapOf("has" to true, "does not have" to false).forEach { (verb, grantPermission) ->
        val expectedResult = if (grantPermission) "succeeds" else "fails"
        listOf(READ, WRITE).forEach { action ->
          context("user $verb ${action.name} access to application") {
            before {
              every {
                permissionEvaluator.hasPermission(any<String>(), application, APPLICATION.name, action.name)
              } returns grantPermission
            }

            test("permission check specifying application name $expectedResult") {
              expectCatching {
                checkApplicationPermission(action, APPLICATION, application)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasApplicationPermission(action.name, APPLICATION.name, application)
              ).isEqualTo(grantPermission)
            }

            test("permission check specifying resource id $expectedResult") {
              expectCatching {
                checkApplicationPermission(action, RESOURCE, locatableResource.id)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasApplicationPermission(action.name, RESOURCE.name, locatableResource.id)
              ).isEqualTo(grantPermission)
            }

            test("permission check specifying delivery config name $expectedResult") {
              expectCatching {
                checkApplicationPermission(action, DELIVERY_CONFIG, deliveryConfig.name)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasApplicationPermission(action.name, DELIVERY_CONFIG.name, deliveryConfig.name)
              ).isEqualTo(grantPermission)
            }
          }

          context("user $verb ${action.name} access to cloud account") {
            before {
              every {
                permissionEvaluator.hasPermission(any<String>(), any(), "ACCOUNT", action.name)
              } returns grantPermission
            }

            test("permission check specifying application name $expectedResult") {
              expectCatching {
                checkCloudAccountPermission(action, APPLICATION, application)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasCloudAccountPermission(action.name, APPLICATION.name, application)
              ).isEqualTo(grantPermission)
            }

            test("permission check specifying resource id $expectedResult") {
              expectCatching {
                checkCloudAccountPermission(action, RESOURCE, locatableResource.id)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasCloudAccountPermission(action.name, RESOURCE.name, locatableResource.id)
              ).isEqualTo(grantPermission)
            }

            test("permission check specifying delivery config name $expectedResult") {
              expectCatching {
                checkCloudAccountPermission(action, DELIVERY_CONFIG, deliveryConfig.name)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasCloudAccountPermission(action.name, DELIVERY_CONFIG.name, deliveryConfig.name)
              ).isEqualTo(grantPermission)
            }
          }

          context("user $verb access to service account") {
            before {
              every {
                permissionEvaluator.hasPermission(any<String>(), any(), "SERVICE_ACCOUNT", any())
              } returns grantPermission
            }

            test("permission check specifying application name $expectedResult") {
              expectCatching {
                checkServiceAccountAccess(APPLICATION, application)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasServiceAccountAccess(APPLICATION.name, application)
              ).isEqualTo(grantPermission)
            }

            test("permission check specifying resource id $expectedResult") {
              expectCatching {
                checkServiceAccountAccess(RESOURCE, locatableResource.id)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasServiceAccountAccess(RESOURCE.name, locatableResource.id)
              ).isEqualTo(grantPermission)
            }

            test("permission check specifying delivery config name $expectedResult") {
              expectCatching {
                checkServiceAccountAccess(DELIVERY_CONFIG, deliveryConfig.name)
              }.passesAsExpected(grantPermission)

              expectThat(
                hasServiceAccountAccess(DELIVERY_CONFIG.name, deliveryConfig.name)
              ).isEqualTo(grantPermission)
            }
          }
        }
      }
    }

    context("authorization is disabled") {
      before {
        every {
          dynamicConfigService.isEnabled("keel.authorization", true)
        } returns false
      }

      test("all application access is allowed") {
        expectCatching {
          checkApplicationPermission(READ, APPLICATION, application)
          checkApplicationPermission(WRITE, APPLICATION, application)
        }.isSuccess()
        expectThat(
          hasApplicationPermission(READ.name, APPLICATION.name, application)
        ).isTrue()
        expectThat(
          hasApplicationPermission(WRITE.name, APPLICATION.name, application)
        ).isTrue()
      }

      test("all cloud account access is allowed") {
        expectCatching {
          checkCloudAccountPermission(READ, APPLICATION, application)
          checkCloudAccountPermission(WRITE, APPLICATION, application)
        }.isSuccess()
        expectThat(
          hasCloudAccountPermission(READ.name, APPLICATION.name, application)
        ).isTrue()
        expectThat(
          hasCloudAccountPermission(WRITE.name, APPLICATION.name, application)
        ).isTrue()
      }

      test("all service account access is allowed") {
        expectCatching {
          checkServiceAccountAccess(APPLICATION, application)
          checkServiceAccountAccess(APPLICATION, application)
        }.isSuccess()
        expectThat(
          hasServiceAccountAccess(APPLICATION.name, application)
        ).isTrue()
        expectThat(
          hasServiceAccountAccess(APPLICATION.name, application)
        ).isTrue()
      }
    }
  }

  private fun DescribeableBuilder<Result<Unit>>.passesAsExpected(grantPermission: Boolean) {
    apply {
      if (grantPermission) isSuccess() else isFailure().isA<AccessDeniedException>()
    }
  }
}
