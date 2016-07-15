package cromwell.engine.workflow.lifecycle.execution

import akka.actor.{ActorRef, LoggingFSM, Props}
import cromwell.backend.BackendJobExecutionActor._
import cromwell.backend.{BackendInitializationData, BackendJobDescriptor, BackendJobDescriptorKey, BackendLifecycleActorFactory}
import cromwell.core.logging.WorkflowLogging
import cromwell.engine.workflow.lifecycle.execution.EngineJobExecutionActor._
import cromwell.engine.workflow.lifecycle.execution.JobPreparationActor.{BackendJobPreparationFailed, BackendJobPreparationSucceeded}
import cromwell.jobstore.JobStoreService.{JobComplete, JobNotComplete, QueryJobCompletion}
import cromwell.jobstore.{Pending => _, _}
import cromwell.services.ServiceRegistryClient

object EngineJobExecutionActor {
  /** States */
  sealed trait EngineJobExecutionActorState
  case object Pending extends EngineJobExecutionActorState
  case object CheckingJobStatus extends EngineJobExecutionActorState
  case object PreparingJob extends EngineJobExecutionActorState
  case object RunningJob extends EngineJobExecutionActorState

  /** Commands */
  sealed trait EngineJobExecutionActorCommand
  final case class Execute(jobKey: BackendJobDescriptorKey) extends EngineJobExecutionActorCommand

  final case class JobRunning(jobDescriptor: BackendJobDescriptor, backendJobExecutionActor: ActorRef)

  def props(jobDescriptorKey: BackendJobDescriptorKey, executionData: WorkflowExecutionActorData, factory: BackendLifecycleActorFactory,
            initializationData: Option[BackendInitializationData], restarting: Boolean) = {
    Props(new EngineJobExecutionActor(jobDescriptorKey, executionData, factory, initializationData, restarting)).withDispatcher("akka.dispatchers.engine-dispatcher")
  }
}

class EngineJobExecutionActor(jobDescriptorKey: BackendJobDescriptorKey,
                              executionData: WorkflowExecutionActorData,
                              factory: BackendLifecycleActorFactory,
                              initializationData: Option[BackendInitializationData],
                              restarting: Boolean) extends LoggingFSM[EngineJobExecutionActorState, Unit] with WorkflowLogging with ServiceRegistryClient {

  override val workflowId = executionData.workflowDescriptor.id

  startWith(Pending, ())

  when(Pending) {
    case Event(Execute(jobKey), _) =>
      if (restarting) {
        serviceRegistryActor ! QueryJobCompletion(jobKey.toJobStoreKey(workflowId))
        goto(CheckingJobStatus)
      } else {
        prepareJob(jobKey)
      }
  }

  when(CheckingJobStatus) {
    case Event(JobNotComplete(_), _) =>
      prepareJob(jobDescriptorKey)
    case Event(JobComplete(_, jobResult), _) =>
      jobResult match {
        case JobResultSuccess(returnCode, jobOutputs) =>
          context.parent ! SucceededResponse(jobDescriptorKey, returnCode, jobOutputs)
          context stop self
          stay()
        case JobResultFailure(returnCode, reason) =>
          context.parent ! FailedNonRetryableResponse(jobDescriptorKey, reason, returnCode)
          context stop self
          stay()
      }
  }

  when(PreparingJob) {
    case Event(BackendJobPreparationSucceeded(jobDescriptor, actorProps), stateData) =>
      val backendJobExecutionActor = context.actorOf(actorProps, buildJobExecutionActorName(jobDescriptor))
      val message = if (restarting) RecoverJobCommand else ExecuteJobCommand
      backendJobExecutionActor ! message
      context.parent ! JobRunning(jobDescriptor, backendJobExecutionActor)
      goto(RunningJob)
    case Event(response: BackendJobPreparationFailed, _) =>
      context.parent forward response
      context stop self
      stay()
  }

  when(RunningJob) {
    case Event(response: BackendJobExecutionResponse, _) =>
      context.parent forward response
      context stop self
      stay()
  }

  def prepareJob(jobKey: BackendJobDescriptorKey) = {
    val jobPreparationActorName = s"$workflowId-BackendPreparationActor-${jobKey.tag}"
    val jobPreparationActor = context.actorOf(JobPreparationActor.props(executionData, jobKey, factory, initializationData), jobPreparationActorName)
    jobPreparationActor ! JobPreparationActor.Start
    goto(PreparingJob)
  }

  private def buildJobExecutionActorName(jobDescriptor: BackendJobDescriptor) = {
    s"$workflowId-BackendJobExecutionActor-${jobDescriptor.key.tag}"
  }
}
