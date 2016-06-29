package cromwell.backend.impl.htcondor

import java.io.Writer
import java.nio.file.{Path, Paths}

import akka.testkit.{ImplicitSender, TestActorRef}
import better.files._
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendJobExecutionActor.{FailedNonRetryableResponse, SucceededResponse}
import cromwell.backend.io.JobPaths
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobDescriptorKey, BackendSpec, BackendWorkflowDescriptor}
import cromwell.core._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}
import wdl4s.values.WdlValue

import scala.sys.process.{Process, ProcessLogger}

class HtCondorJobExecutionActorSpec extends TestKitSuite("HtCondorJobExecutionActorSpec")
  with WordSpecLike
  with Matchers
  with MockitoSugar
  with ImplicitSender {

  import BackendSpec._

  private val htCondorCommands: HtCondorCommands = new HtCondorCommands
  private val htCondorProcess: HtCondorProcess = mock[HtCondorProcess]

  private val helloWorldWdl =
    """
      |task hello {
      |  command {
      |    echo "Hello World!"
      |  }
      |  output {
      |    String salutation = read_string(stdout())
      |  }
      |  RUNTIME
      |}
      |
      |workflow hello {
      |  call hello
      |}
    """.stripMargin
  private val backendConfig = ConfigFactory.parseString(
    s"""{
        |  root = "local-cromwell-executions"
        |  filesystems {
        |    local {
        |      localization = [
        |        "hard-link", "soft-link", "copy"
        |      ]
        |    }
        |  }
        |}
        """.stripMargin)


  private case class TestJobDescriptor(jobDescriptor: BackendJobDescriptor, jobPaths: JobPaths, backendConfigurationDescriptor: BackendConfigurationDescriptor)

  private def prepareJob(runtimeString: String = ""): TestJobDescriptor = {
    val backendWorkflowDescriptor = buildWorkflowDescriptor(wdl = helloWorldWdl, runtime = runtimeString)
    val backendConfigurationDescriptor = BackendConfigurationDescriptor(backendConfig, ConfigFactory.load)
    val jobDesc = jobDescriptorFromSingleCallWorkflow(backendWorkflowDescriptor)
    val jobPaths = new JobPaths(backendWorkflowDescriptor, backendConfig, jobDesc.key)
    val executionDir = jobPaths.callRoot
    val stdout = Paths.get(executionDir.path.toString, "stdout")
    stdout.toString.toFile.createIfNotExists(false)
    val submitFileStderr = executionDir.resolve("submitfile.stderr")
    val submitFileStdout = executionDir.resolve("submitfile.stdout")
    submitFileStdout.toString.toFile.createIfNotExists(false)
    submitFileStdout <<
      """Submitting job(s)..
        |1 job(s) submitted to cluster 88.
      """.stripMargin.trim
    submitFileStderr.toString.toFile.createIfNotExists(false)
    TestJobDescriptor(jobDesc,jobPaths, backendConfigurationDescriptor)
  }

  private def cleanUpJob(jobPaths: JobPaths): Unit = {
    jobPaths.workflowRoot.delete(true)
  }

  private def jobDescriptorFromSingleCallWorkflow(workflowDescriptor: BackendWorkflowDescriptor,
                                                  inputs: Map[String, WdlValue] = Map.empty) = {
    val call = workflowDescriptor.workflowNamespace.workflow.calls.head
    val jobKey = new BackendJobDescriptorKey(call, None, 1)
    new BackendJobDescriptor(workflowDescriptor, jobKey, inputs)
  }

  trait MockWriter extends Writer {
    var closed = false

    override def close() = closed = true

    override def flush() = {}

    override def write(a: Array[Char], b: Int, c: Int) = {}
  }

  "executeTask method" should {
    "return succeeded task status with stdout " in {
      val jobDescriptor = prepareJob()
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(htCondorProcess.commandList(any[String])).thenReturn(Seq.empty[String])
      when(htCondorProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(htCondorProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(htCondorProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)
      when(htCondorProcess.processStderr).thenReturn(stderrResult)

      val backend = TestActorRef(new HtCondorJobExecutionActor(job, backendConfigDesc) {
        override lazy val cmds = htCondorCommands
        override lazy val extProcess = htCondorProcess
      }).underlyingActor

      whenReady(backend.execute) { response =>
        response shouldBe a[SucceededResponse]
        verify(htCondorProcess, times(1)).externalProcess(any[Seq[String]], any[ProcessLogger])
        verify(htCondorProcess, times(1)).tailedWriter(any[Int], any[Path])
        verify(htCondorProcess, times(1)).untailedWriter(any[Path])
      }

      cleanUpJob(jobPaths)
    }
  }

  "executeTask method" should {
    "return failed task status with stderr on non-zero process exit" in {
      val jobDescriptor = prepareJob()
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)

      val backend = TestActorRef(new HtCondorJobExecutionActor(job, backendConfigDesc) {
        override lazy val cmds = htCondorCommands
        override lazy val extProcess = htCondorProcess
      }).underlyingActor
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(htCondorProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(htCondorProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(htCondorProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)
      when(htCondorProcess.processStderr).thenReturn(stderrResult)
      when(htCondorProcess.jobReturnCode(any[String], any[Path])).thenReturn(-1)

      whenReady(backend.execute) { response =>
        response shouldBe a[FailedNonRetryableResponse]
        assert(response.asInstanceOf[FailedNonRetryableResponse].throwable.getMessage.contains("Job exited with invalid return code"))
      }

      cleanUpJob(jobPaths)
    }
  }

  "executeTask method" should {
    "return a successful task status even with a non-zero process exit" in {
      val runtime =
        """
          |runtime {
          | continueOnReturnCode: [911]
          |}
        """.stripMargin
      val jobDescriptor = prepareJob(runtimeString = runtime)
      val (job, jobPaths, backendConfigDesc) = (jobDescriptor.jobDescriptor, jobDescriptor.jobPaths, jobDescriptor.backendConfigurationDescriptor)

      val backend = TestActorRef(new HtCondorJobExecutionActor(job, backendConfigDesc) {
        override lazy val cmds = htCondorCommands
        override lazy val extProcess = htCondorProcess
      }).underlyingActor
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(jobPaths.stdout) with MockPathWriter
      val stubTailed = new TailedWriter(jobPaths.stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(htCondorProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(htCondorProcess.tailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(htCondorProcess.untailedWriter(any[Path])).thenReturn(stubUntailed)
      when(htCondorProcess.processStderr).thenReturn(stderrResult)
      when(htCondorProcess.jobReturnCode(any[String], any[Path])).thenReturn(911)

      whenReady(backend.execute) { response =>
        response shouldBe a[SucceededResponse]
      }

      cleanUpJob(jobPaths)
    }
  }

  trait MockPathWriter extends PathWriter {
    override lazy val writer: Writer = new MockWriter {}
    override val path: Path = mock[Path]
  }
}
