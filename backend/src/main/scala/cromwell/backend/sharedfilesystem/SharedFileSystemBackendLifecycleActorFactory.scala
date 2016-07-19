package cromwell.backend.sharedfilesystem

import akka.actor.Props
import cromwell.backend.BackendJobExecutionActor.BackendJobExecutionResponse
import cromwell.backend.io.SharedFsExpressionFunctions
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendJobDescriptor, BackendJobDescriptorKey, BackendLifecycleActorFactory, BackendWorkflowDescriptor}
import wdl4s.Call
import wdl4s.expression.WdlStandardLibraryFunctions

import scala.concurrent.Promise

trait SharedFileSystemBackendLifecycleActorFactory extends BackendLifecycleActorFactory {

  def configurationDescriptor: BackendConfigurationDescriptor

  def asyncJobExecutionActorProps(params: SharedFileSystemAsyncJobExecutionActorParams): Props

  def runtimeAttributesBuilder: SharedFileSystemRuntimeAttributesBuilder[SharedFileSystemRuntimeAttributes]

  override def workflowInitializationActorProps(workflowDescriptor: BackendWorkflowDescriptor,
                                                calls: Seq[Call]): Option[Props] = {
    val params = SharedFileSystemInitializationActorParams(workflowDescriptor, configurationDescriptor, calls,
      runtimeAttributesBuilder)
    Option(Props(new SharedFileSystemInitializationActor(params)))
  }

  override def jobExecutionActorProps(jobDescriptor: BackendJobDescriptor,
                                      initializationDataOption: Option[BackendInitializationData]): Props = {
    def propsCreator(completionPromise: Promise[BackendJobExecutionResponse]): Props = {
      val params = SharedFileSystemAsyncJobExecutionActorParams(jobDescriptor, configurationDescriptor,
        completionPromise, initializationDataOption)
      asyncJobExecutionActorProps(params)
    }

    Props(new SharedFileSystemJobExecutionActor(jobDescriptor, configurationDescriptor, propsCreator))
  }

  override def expressionLanguageFunctions(workflowDescriptor: BackendWorkflowDescriptor,
                                           jobKey: BackendJobDescriptorKey,
                                           initializationData: Option[BackendInitializationData]):
  WdlStandardLibraryFunctions = {
    SharedFsExpressionFunctions(workflowDescriptor, configurationDescriptor, jobKey, initializationData)
  }
}

case class SharedFileSystemAsyncJobExecutionActorParams
(
  jobDescriptor: BackendJobDescriptor,
  configurationDescriptor: BackendConfigurationDescriptor,
  completionPromise: Promise[BackendJobExecutionResponse],
  backendInitializationDataOption: Option[BackendInitializationData]
)
