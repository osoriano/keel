package com.netflix.spinnaker.keel.veto

import com.netflix.spinnaker.keel.test.resource
import dev.minutest.junit.JUnit5Minutests
import dev.minutest.rootContext
import kotlinx.coroutines.runBlocking
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue

class VetoEnforcerTests : JUnit5Minutests {

  internal class Fixture(
    val veto: DummyVeto
  ) {
    val subject = VetoEnforcer(listOf(veto))

    val r = resource()

    fun canActuate() = runBlocking { subject.canActuate(r) }
  }

  fun allowTests() = rootContext<Fixture> {
    fixture {
      Fixture(
        DummyVeto(true)
      )
    }

    context("always allow veto") {
      test("resource gets checked") {
        val response = canActuate()
        expectThat(response.allowed).isTrue()
      }
    }

    fun denyTests() = rootContext<Fixture> {
      fixture {
        Fixture(
          DummyVeto(false)
        )
      }

      context("always deny veto") {
        test("when we have one deny we deny overall") {
          val response = canActuate()
          expectThat(response.allowed).isFalse()
        }
      }
    }
  }
}