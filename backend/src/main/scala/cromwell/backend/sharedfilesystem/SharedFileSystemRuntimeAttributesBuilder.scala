package cromwell.backend.sharedfilesystem

import cromwell.backend.validation._
import cromwell.core._
import wdl4s.values.WdlValue

trait SharedFileSystemRuntimeAttributesBuilder[+A] extends RuntimeAttributesBuilder[A] {
  private[sharedfilesystem] val requiredValidations: Set[RuntimeAttributesValidation[_]] =
    Set(ContinueOnReturnCodeValidation, FailOnStderrValidation)

  protected def customValidations: Traversable[RuntimeAttributesValidation[_]] = Seq.empty

  override final lazy val validations = requiredValidations ++ customValidations

  private[sharedfilesystem] def validateInternal(values: Map[String, WdlValue]): ErrorOr[A] = validate(values)

  final def withDockerSupport(supportsDocker: Boolean): SharedFileSystemRuntimeAttributesBuilder[A] = {
    import SharedFileSystemRuntimeAttributesBuilder._
    if (supportsDocker) withDocker(this) else withoutDocker(this)
  }
}

object SharedFileSystemRuntimeAttributesBuilder {
  def withDocker[A](builder: SharedFileSystemRuntimeAttributesBuilder[A]):
  SharedFileSystemRuntimeAttributesBuilder[A] = {
    new SharedFileSystemRuntimeAttributesBuilder[A] {
      override protected[backend] val requiredValidations = builder.requiredValidations + DockerValidation.optional

      override protected def validate(values: Map[String, WdlValue]) = builder.validateInternal(values)
    }
  }

  def withoutDocker[A](builder: SharedFileSystemRuntimeAttributesBuilder[A]):
  SharedFileSystemRuntimeAttributesBuilder[A] = {
    new SharedFileSystemRuntimeAttributesBuilder[A] {
      override protected[backend] val requiredValidations = builder.requiredValidations - DockerValidation.optional

      override protected def validate(values: Map[String, WdlValue]) = builder.validateInternal(values)
    }
  }
}
