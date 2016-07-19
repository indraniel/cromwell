package cromwell.backend.impl.sge

import cromwell.backend.sharedfilesystem.{SharedFileSystemRuntimeAttributes, SharedFileSystemRuntimeAttributesBuilder}
import cromwell.backend.MemorySize
import cromwell.backend.validation._
import cromwell.core._
import wdl4s.values.WdlValue

import scalaz.Scalaz._

case class SgeRuntimeAttributes(dockerImageOption: Option[String],
                                failOnStderr: Boolean,
                                continueOnReturnCode: ContinueOnReturnCode,
                                memorySizeOption: Option[MemorySize]) extends SharedFileSystemRuntimeAttributes {
  override def asMap = super.asMap ++ optionalMap(MemoryValidation.Optional.key, memorySizeOption)
}

object SgeRuntimeAttributesBuilder extends SharedFileSystemRuntimeAttributesBuilder[SgeRuntimeAttributes] {
  override protected val customValidations = Seq(MemoryValidation.Optional)

  override protected def validate(values: Map[String, WdlValue]): ErrorOr[SgeRuntimeAttributes] = {
    val dockerValidationOptionErrorOr: ErrorOr[Option[String]] = DockerValidation.optional.validate(values)
    val failOnStderrErrorOr: ErrorOr[Boolean] = FailOnStderrValidation.validate(values)
    val continueOnReturnCodeErrorOr: ErrorOr[ContinueOnReturnCode] = ContinueOnReturnCodeValidation.validate(values)
    val memoryErrorOr: ErrorOr[Option[MemorySize]] = MemoryValidation.Optional.validate(values)
    (dockerValidationOptionErrorOr |@| failOnStderrErrorOr |@| continueOnReturnCodeErrorOr |@| memoryErrorOr) {
      SgeRuntimeAttributes.apply
    }
  }
}
