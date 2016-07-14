package cromwell.jobstore

import akka.actor.Actor
import com.typesafe.config.Config
import cromwell.backend.BackendJobDescriptorKey
import cromwell.core.WorkflowId
import cromwell.jobstore.JobStoreService.{JobStoreReaderCommand, JobStoreWriterCommand}
import cromwell.services.ServiceRegistryActor.ServiceRegistryMessage


/**
  * Joins the service registry API to the JobStoreReaderActor and JobStoreWriterActor.
  *
  * This level of indirection is a tiny bit awkward but allows the database to be injected.
  */
case class JobStoreService(serviceConfig: Config, globalConfig: Config) extends Actor {

  // TODO: Replace with a real database, probably from config.
  val database = FilesystemJobStoreDatabase
  val jobStoreWriterActor = context.actorOf(JobStoreWriterActor.props(database))
  val jobStoreReaderActor = context.actorOf(JobStoreReaderActor.props(database))

  override def receive: Receive = {
    case command: JobStoreWriterCommand => jobStoreWriterActor.tell(command, sender())
    case command: JobStoreReaderCommand => jobStoreReaderActor.tell(command, sender())
  }
}

object JobStoreService {
  sealed trait JobStoreCommand extends ServiceRegistryMessage { override def serviceName: String = "JobStore"}

  sealed trait JobStoreWriterCommand extends JobStoreCommand
  case class RegisterJobCompleted(jobKey: JobStoreKey, jobResult: JobResult) extends JobStoreWriterCommand
  case class RegisterWorkflowCompleted(workflowId: WorkflowId) extends JobStoreWriterCommand

  sealed trait JobStoreWriterResponse
  case class JobStoreWriteSuccess(originalCommand: JobStoreWriterCommand) extends JobStoreWriterResponse
  case class JobStoreWriteFailure(originalCommand: JobStoreWriterCommand, reason: Throwable) extends JobStoreWriterResponse

  sealed trait JobStoreReaderCommand extends JobStoreCommand
  /**
    * Message to query the JobStoreReaderActor, asks whether the specified job has already been completed.
    */
  case class QueryJobCompletion(workflowId: WorkflowId, jobKey: BackendJobDescriptorKey) extends JobStoreReaderCommand

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
