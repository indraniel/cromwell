package cromwell.backend.sharedfilesystem

import better.files._
import cromwell.backend.io.WorkflowPaths
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendWorkflowDescriptor, BackendWorkflowInitializationActor}
import wdl4s.{Call, WdlExpression}

import scala.concurrent.Future
import scala.util.Try

class SharedFileSystemInitializationActor(params: SharedFileSystemInitializationActorParams)
  extends BackendWorkflowInitializationActor {

  override lazy val workflowDescriptor: BackendWorkflowDescriptor = params.workflowDescriptor
  override lazy val configurationDescriptor: BackendConfigurationDescriptor = params.configurationDescriptor
  override lazy val calls: Seq[Call] = params.calls
  lazy val runtimeAttributesBuilder: SharedFileSystemRuntimeAttributesBuilder[_] = params.runtimeAttributesBuilder

  override protected def runtimeAttributeValidators: Map[String, (Option[WdlExpression]) => Boolean] = {
    runtimeAttributesBuilder.validations.map(validation =>
      validation.key -> validation.validateOptionalExpression _
    ).toMap
  }

  private val workflowPaths = new WorkflowPaths(workflowDescriptor, configurationDescriptor.backendConfig, None)

  /**
    * A call which happens before anything else runs
    */
  override def beforeAll(): Future[Option[BackendInitializationData]] = {
    Future.fromTry(Try {
      publishWorkflowRoot(workflowPaths.workflowRoot.toString)
      workflowPaths.workflowRoot.createDirectories()
      None
    })
  }

  /**
    * Log a warning if there are non-supported runtime attributes defined for the call.
    */
  override def validate(): Future[Unit] = {
    Future.fromTry(Try {
      calls foreach { call =>
        val runtimeAttributeKeys = call.task.runtimeAttributes.attrs.keys.toList
        val notSupportedAttributes = runtimeAttributeKeys.diff(runtimeAttributesBuilder.validationKeys.toList)

        if (notSupportedAttributes.nonEmpty) {
          val notSupportedAttrString = notSupportedAttributes mkString ", "
          workflowLogger.warn(
            s"Key/s [$notSupportedAttrString] is/are not supported by backend. " +
              s"Unsupported attributes will not be part of jobs executions.")
        }
      }
    })
  }
}

case class SharedFileSystemInitializationActorParams
(
  workflowDescriptor: BackendWorkflowDescriptor,
  configurationDescriptor: BackendConfigurationDescriptor,
  calls: Seq[Call],
  runtimeAttributesBuilder: SharedFileSystemRuntimeAttributesBuilder[SharedFileSystemRuntimeAttributes]
)
