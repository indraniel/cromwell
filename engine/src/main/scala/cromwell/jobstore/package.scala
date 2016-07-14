package cromwell

import cromwell.backend.BackendJobDescriptorKey
import cromwell.core.{JobOutput, JobOutputs, WorkflowId}
import cromwell.webservice.WdlValueJsonFormatter.WdlValueJsonFormat
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat, _}

package object jobstore {
  case class JobStoreKey(workflowId: WorkflowId, callFqn: String, index: Option[Int], attempt: Int)

  sealed trait JobResult
  case class JobResultSuccess(returnCode: Option[Int], jobOutputs: JobOutputs) extends JobResult
  case class JobResultFailure(returnCode: Option[Int], reason: Throwable) extends JobResult

  implicit class EnhancedBackendJobDescriptorKey(val backendKey: BackendJobDescriptorKey) extends AnyVal {
    def toStoreKey(workflowId: WorkflowId): JobStoreKey = JobStoreKey(workflowId, backendKey.call.fullyQualifiedName, backendKey.index, backendKey.attempt)
  }

  object JobResultJsonFormatter extends DefaultJsonProtocol {
    implicit object ThrowableFormat extends RootJsonFormat[Throwable] {
      def write(value: Throwable) = value.getMessage.toJson
      def read(value: JsValue): Throwable = new Exception(value.convertTo[String])
    }

    implicit object JobOutputFormat extends RootJsonFormat[JobOutput] {
      def write(value: JobOutput) = value.wdlValue.toJson
      def read(value: JsValue): JobOutput = JobOutput(WdlValueJsonFormat.read(value), None)
    }

    implicit val JobResultSuccessFormat = jsonFormat2(JobResultSuccess)
    implicit val JobResultFailureFormat = jsonFormat2(JobResultFailure)

    implicit object JobResultFormat extends RootJsonFormat[JobResult] {
      def write(value: JobResult) = JsObject(
        value.getClass.getSimpleName -> (value match {
          case x: JobResultSuccess => x.toJson
          case x: JobResultFailure => x.toJson
        })
      )
      def read(value: JsValue): JobResult = {
        val fields = value.asJsObject.fields
        if (fields.contains("JobResultSuccess")) {
          fields("JobResultSuccess").convertTo[JobResultSuccess]
        } else {
          fields("JobResultFailure").convertTo[JobResultFailure]
        }
      }
    }
  }
}
