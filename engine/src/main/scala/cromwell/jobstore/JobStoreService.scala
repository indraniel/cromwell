package cromwell.jobstore

import akka.actor.Actor
import com.typesafe.config.Config
import cromwell.backend.BackendJobDescriptorKey
import cromwell.core.WorkflowId
import cromwell.jobstore.JobStoreService.{JobStoreReaderServiceCommand, JobStoreWriterServiceCommand}
import cromwell.services.ServiceRegistryActor.ServiceRegistryMessage


/**
  * Joins the service registry API to the JobStoreWriterActor.
  *
  * This level of indirection is a tiny bit awkward but allows the database to be injected.
  */
case class JobStoreService(serviceConfig: Config, globalConfig: Config) extends Actor {

  // TODO: Replace with a real database, probably from config.
  val database = FilesystemJobStoreDatabase
  val jobStoreWriterActor = context.actorOf(JobStoreWriterActor.props(database))
  val jobStoreReaderActor = context.actorOf(JobStoreReaderActor.props(database))

  override def receive: Receive = {
    case command: JobStoreWriterServiceCommand => jobStoreWriterActor.tell(command, sender())
    case command: JobStoreReaderServiceCommand => jobStoreReaderActor.tell(command, sender())
  }
}

object JobStoreService {
  sealed trait JobStoreCommand extends ServiceRegistryMessage { override def serviceName: String = "JobStore"}

  sealed trait JobStoreWriterServiceCommand extends JobStoreCommand
  case class RegisterJobCompleted(jobKey: JobStoreKey, jobResult: JobResult) extends JobStoreWriterServiceCommand
  case class RegisterWorkflowCompleted(workflowId: WorkflowId) extends JobStoreWriterServiceCommand

  sealed trait JobStoreWriterServiceResponse
  case class JobStoreWriteSuccess(originalCommand: JobStoreWriterServiceCommand) extends JobStoreWriterServiceResponse
  case class JobStoreWriteFailure(originalCommand: JobStoreWriterServiceCommand, reason: Throwable) extends JobStoreWriterServiceResponse

  sealed trait JobStoreReaderServiceCommand extends JobStoreCommand
  /**
    * Message to query the JobStoreReaderActor, asks whether the specified job has already been completed.
    */
  case class QueryJobCompletion(workflowId: WorkflowId, jobKey: BackendJobDescriptorKey) extends JobStoreReaderServiceCommand

  sealed trait JobStoreReaderResponse
  /**
    * Message which indicates that a job has already completed, and contains the results of the job
    */
  case class JobComplete(jobKey: BackendJobDescriptorKey, jobResult: JobResult) extends JobStoreReaderResponse
  /**
    * Indicates that the job has not been completed yet. Makes no statement about whether the job is
    * running versus unstarted or (maybe?) doesn't even exist!
    */
  case class JobNotComplete(jobKey: BackendJobDescriptorKey) extends JobStoreReaderResponse

  case class JobStoreReadFailure(jobDescriptorKey: BackendJobDescriptorKey, reason: Throwable) extends JobStoreReaderResponse
}
