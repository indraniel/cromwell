package cromwell.backend.impl.sge

import akka.actor.Props
import akka.testkit.{EventFilter, ImplicitSender, TestDuration}
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendWorkflowInitializationActor.Initialize
import cromwell.backend.sharedfilesystem.{SharedFileSystemInitializationActor, SharedFileSystemInitializationActorParams}
import cromwell.backend.{BackendConfigurationDescriptor, BackendSpec, BackendWorkflowDescriptor}
import cromwell.core.TestKitSuite
import cromwell.core.logging.LoggingTest._
import org.scalatest.{Matchers, WordSpecLike}
import wdl4s.Call

import scala.concurrent.duration._

class SgeInitializationActorSpec extends TestKitSuite("SgeInitializationActorSpec") with WordSpecLike with Matchers
  with ImplicitSender {
  val Timeout = 5.second.dilated

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

  private def getSgeBackend(workflowDescriptor: BackendWorkflowDescriptor, calls: Seq[Call], conf: BackendConfigurationDescriptor) = {
    val params = SharedFileSystemInitializationActorParams(workflowDescriptor, conf, calls, SgeRuntimeAttributesBuilder)
    val props = Props(new SharedFileSystemInitializationActor(params))
    system.actorOf(props)
  }

  "SgeInitializationActor" should {
    "log a warning message when there are unsupported runtime attributes" in {
      within(Timeout) {
        val workflowDescriptor = buildWorkflowDescriptor(HelloWorld, runtime = """runtime { unsupported: 1 }""")
        val conf = BackendConfigurationDescriptor(
          ConfigFactory.parseString("{root: cromwell-test-executions}"), ConfigFactory.load())
        val backend = getSgeBackend(workflowDescriptor, workflowDescriptor.workflowNamespace.workflow.calls, conf)
        val pattern = "Key/s [unsupported] is/are not supported by backend. " +
          "Unsupported attributes will not be part of jobs executions."
        EventFilter.warning(pattern = escapePattern(pattern), occurrences = 1) intercept {
          backend ! Initialize
        }
      }
    }
  }
}
