package cromwell.backend.impl.sge

import ch.qos.logback.classic.Level
import cromwell.backend.BackendSpec
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.backend.validation.{ContinueOnReturnCode, ContinueOnReturnCodeSet}
import cromwell.core.WorkflowOptions
import lenthall.test.logging.TestLogger
import org.scalatest.{Matchers, WordSpecLike}
import org.slf4j.LoggerFactory
import wdl4s.values.WdlValue

class SgeRuntimeAttributesSpec extends WordSpecLike with Matchers {

  import BackendSpec._

  val HelloWorld =
    """
      |task hello {
      |  String addressee = "you"
      |  command {
      |    echo "Hello ${addressee}!"
      |  }
      |  output {
      |    String salutation = read_string(stdout())
      |  }
      |
      |  RUNTIME
      |}
      |
      |workflow hello {
      |  call hello
      |}
    """.stripMargin

  val defaultRuntimeAttributes = Map(
    DockerKey -> None,
    FailOnStderrKey -> false,
    ContinueOnReturnCodeKey -> ContinueOnReturnCodeSet(Set(0)))

  "SgeRuntimeAttributes" should {
    "return an instance of itself when there are no runtime attributes defined." in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { }""").head
      assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, defaultRuntimeAttributes)
    }

    "return an instance of itself when tries to validate a valid Docker entry" in {
      val expectedRuntimeAttributes = defaultRuntimeAttributes + (DockerKey -> Option("ubuntu:latest"))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { docker: "ubuntu:latest" }""").head
      assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, expectedRuntimeAttributes)
    }

    "return an instance of itself when tries to validate a valid Docker entry based on input" in {
      val expectedRuntimeAttributes = defaultRuntimeAttributes + (DockerKey -> Option("you"))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { docker: "\${addressee}" }""").head
      assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, expectedRuntimeAttributes)
    }

    "throw an exception when tries to validate an invalid Docker entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { docker: 1 }""").head
      assertSgeRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting docker runtime attribute to be a String")
    }

    "return an instance of itself when tries to validate a valid failOnStderr entry" in {
      val expectedRuntimeAttributes = defaultRuntimeAttributes + (FailOnStderrKey -> true)
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { failOnStderr: "true" }""").head
      assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, expectedRuntimeAttributes)
    }

    "log a warning and return an instance of itself when tries to validate a valid Docker entry" in {
      val expectedRuntimeAttributes = defaultRuntimeAttributes + (DockerKey -> Option("ubuntu:latest"))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { docker: "ubuntu:latest" }""").head
      TestLogger.withTestLoggerFor(defaultLogger) { expectedLogger =>
        expectedLogger.setLevel(Level.WARN)
        assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, expectedRuntimeAttributes,
          supportsDocker = false)
        expectedLogger.messages should include("[WARN] Unrecognized runtime attribute keys: docker\n")
      }
    }

    "log a warning and return an instance of itself when tries to validate a valid Docker entry based on input" in {
      val expectedRuntimeAttributes = defaultRuntimeAttributes + (DockerKey -> Option("you"))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { docker: "\${addressee}" }""").head
      TestLogger.withTestLoggerFor(defaultLogger) { expectedLogger =>
        expectedLogger.setLevel(Level.WARN)
        assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, expectedRuntimeAttributes,
          supportsDocker = false)
        expectedLogger.messages should include("[WARN] Unrecognized runtime attribute keys: docker\n")
      }
    }

    "log a warning and throw an exception when tries to validate an invalid Docker entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { docker: 1 }""").head
      TestLogger.withTestLoggerFor(defaultLogger) { expectedLogger =>
        expectedLogger.setLevel(Level.WARN)
        assertSgeRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting docker runtime attribute to be a String",
          supportsDocker = false)
        expectedLogger.messages should include("[WARN] Unrecognized runtime attribute keys: docker\n")
      }
    }

    "throw an exception when tries to validate an invalid failOnStderr entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { failOnStderr: "yes" }""").head
      assertSgeRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting failOnStderr runtime attribute to be a Boolean or a String with values of 'true' or 'false'")
    }

    "return an instance of itself when tries to validate a valid continueOnReturnCode entry" in {
      val expectedRuntimeAttributes = defaultRuntimeAttributes + (ContinueOnReturnCodeKey -> ContinueOnReturnCodeSet(Set(1)))
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { continueOnReturnCode: 1 }""").head
      assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes, expectedRuntimeAttributes)
    }

    "throw an exception when tries to validate an invalid continueOnReturnCode entry" in {
      val runtimeAttributes = createRuntimeAttributes(HelloWorld, """runtime { continueOnReturnCode: "value" }""").head
      assertSgeRuntimeAttributesFailedCreation(runtimeAttributes, "Expecting continueOnReturnCode runtime attribute to be either a Boolean, a String 'true' or 'false', or an Array[Int]")
    }

  }

  val defaultLogger = LoggerFactory.getLogger(classOf[SgeRuntimeAttributesSpec])
  val emptyWorkflowOptions = WorkflowOptions.fromMap(Map.empty).get

  private def assertSgeRuntimeAttributesSuccessfulCreation(runtimeAttributes: Map[String, WdlValue],
                                                           expectedRuntimeAttributes: Map[String, Any],
                                                           supportsDocker: Boolean = false): Unit = {
    try {
      val sgeRuntimeAttributes = SgeRuntimeAttributesBuilder.withDockerSupport(supportsDocker).
        build(runtimeAttributes, emptyWorkflowOptions, defaultLogger)
      assert(sgeRuntimeAttributes.dockerImageOption ==
        expectedRuntimeAttributes(DockerKey).asInstanceOf[Option[String]])
      assert(sgeRuntimeAttributes.failOnStderr == expectedRuntimeAttributes(FailOnStderrKey).asInstanceOf[Boolean])
      assert(sgeRuntimeAttributes.continueOnReturnCode ==
        expectedRuntimeAttributes(ContinueOnReturnCodeKey).asInstanceOf[ContinueOnReturnCode])
    } catch {
      case ex: RuntimeException => fail(s"Exception was not expected but received: ${ex.getMessage}")
    }
  }

  private def assertSgeRuntimeAttributesFailedCreation(runtimeAttributes: Map[String, WdlValue], exMsg: String,
                                                       supportsDocker: Boolean = true): Unit = {
    try {
      SgeRuntimeAttributesBuilder.withDockerSupport(supportsDocker).
        build(runtimeAttributes, emptyWorkflowOptions, defaultLogger)
      fail("A RuntimeException was expected.")
    } catch {
      case ex: RuntimeException => assert(ex.getMessage.contains(exMsg))
    }
  }
}
