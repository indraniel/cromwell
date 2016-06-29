package cromwell

import lenthall.exception.ThrowableAggregation
import java.nio.file.Path

import wdl4s.values.{SymbolHash, WdlValue}

import scalaz._

package object core {
  // root can be a Path instead of a String in PBE. stdout / err too but it doesn't really bring values since they're just stringified to WdlFiles
  class OldWorkflowContext(val root: String)
  class OldCallContext(override val root: String, val stdout: String, val stderr: String) extends OldWorkflowContext(root)

  case class CallContext(root: Path, stdout: String, stderr: String)

  type ErrorOr[+A] = ValidationNel[String, A]
  type LocallyQualifiedName = String
  type FullyQualifiedName = String
  type WorkflowOutputs = Map[FullyQualifiedName, JobOutput]
  type WorkflowOptionsJson = String
  case class JobOutput(wdlValue: WdlValue, @deprecated("Don't return hashes to the engine!", "PBE") hash: Option[SymbolHash] = None) // TODO: Remove this hash
  type JobOutputs = Map[LocallyQualifiedName, JobOutput]
  type HostInputs = Map[String, WdlValue]
  type EvaluatedRuntimeAttributes = Map[String, WdlValue]

  class CromwellFatalException(exception: Throwable) extends Exception(exception)
  case class CromwellAggregatedException(throwables: Seq[Throwable], exceptionContext: String = "") extends ThrowableAggregation
}
