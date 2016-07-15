package cromwell.jobstore

import akka.actor.ActorRef
import cromwell.CromwellTestkitSpec
import cromwell.core.WorkflowId
import cromwell.jobstore.JobStoreService.{JobStoreWriteSuccess, RegisterJobCompleted, RegisterWorkflowCompleted}
import org.scalatest.{BeforeAndAfter, Matchers}

import scala.concurrent.duration._
import language.postfixOps
import scala.concurrent.{ExecutionContext, Future}

class JobStoreWriterSpec extends CromwellTestkitSpec with Matchers with BeforeAndAfter {
  
  var database: WriteCountingJobStoreDatabase = _
  var jobStoreWriter: ActorRef = _
  var workflowId: WorkflowId = _
  val successResult: JobResult = JobResultSuccess(Some(0), Map.empty)

  before {
    database = WriteCountingJobStoreDatabase.makeNew
    jobStoreWriter = system.actorOf(JobStoreWriterActor.props(database))
  }

  private def registerCompletions(attempts: Int): Unit = {
    0 until attempts foreach { a => jobStoreWriter ! RegisterJobCompleted(jobKey(attempt = a), successResult) }
  }

  private def jobKey(attempt: Int): JobStoreKey = JobStoreKey(workflowId, s"call.fqn", None, attempt)

  private def assertWriteSuccess(key: JobStoreKey, result: JobResult): Unit = {
    key.workflowId shouldBe workflowId
    key.callFqn shouldBe "call.fqn"
    key.index shouldBe None
    result shouldBe successResult
  }

  private def assertDb(totalWritesCalled: Int, jobCompletionsRecorded: Int, workflowCompletionsRecorded: Int): Unit = {
    database.totalWritesCalled shouldBe totalWritesCalled
    database.jobCompletionsRecorded shouldBe jobCompletionsRecorded
    database.workflowCompletionsRecorded shouldBe workflowCompletionsRecorded
  }

  private def assertReceived(allowWorkflowCompleted: Boolean): Unit = {
    val received = receiveN(3, 10 seconds)
    received foreach {
      case JobStoreWriteSuccess(RegisterJobCompleted(key: JobStoreKey, result)) => assertWriteSuccess(key, result)
      case JobStoreWriteSuccess(RegisterWorkflowCompleted(id)) if allowWorkflowCompleted => id shouldBe workflowId
      case message => fail(s"Unexpected response message: $message")
    }
  }

  "JobStoreWriter" should {
    "be able to collapse writes together if they arrive while a database access is ongoing" in {

      registerCompletions(attempts = 3)
      assertReceived(allowWorkflowCompleted = false)

      assertDb(
        totalWritesCalled = 2,
        jobCompletionsRecorded = 3,
        workflowCompletionsRecorded = 0
      )
    }

    "be able to skip job-completion writes if the workflow completes, but still respond appropriately" in {

      registerCompletions(attempts = 2)
      jobStoreWriter ! RegisterWorkflowCompleted(workflowId)
      assertReceived(allowWorkflowCompleted = true)

      assertDb(
        totalWritesCalled = 2,
        jobCompletionsRecorded = 1,
        workflowCompletionsRecorded = 1
      )
    }
  }
}

class WriteCountingJobStoreDatabase(var totalWritesCalled: Int, var jobCompletionsRecorded: Int, var workflowCompletionsRecorded: Int) extends JobStoreDatabase {

  override def writeToDatabase(jobCompletions: Map[JobStoreKey, JobResult], workflowCompletions: List[WorkflowId])
                              (implicit ec: ExecutionContext): Future[Unit] = {
    totalWritesCalled += 1
    jobCompletionsRecorded += jobCompletions.size
    workflowCompletionsRecorded += workflowCompletions.size
    Future.successful(())
  }

  override def readJobResult(jobStoreKey: JobStoreKey)(implicit ec: ExecutionContext): Future[Option[JobResult]] = throw new NotImplementedError()
}

object WriteCountingJobStoreDatabase {
  def makeNew = new WriteCountingJobStoreDatabase(0, 0, 0)
}
