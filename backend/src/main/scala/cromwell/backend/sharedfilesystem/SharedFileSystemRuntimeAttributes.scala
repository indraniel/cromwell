package cromwell.backend.sharedfilesystem

import cromwell.backend.validation.{ContinueOnReturnCode, ContinueOnReturnCodeValidation, DockerValidation, FailOnStderrValidation}

trait SharedFileSystemRuntimeAttributes {
  def dockerImageOption: Option[String]
  def failOnStderr: Boolean
  def continueOnReturnCode: ContinueOnReturnCode

  def asMap: Map[String, Any] = Map(
    DockerValidation.key -> dockerImageOption,
    FailOnStderrValidation.key -> failOnStderr,
    ContinueOnReturnCodeValidation.key -> continueOnReturnCode
  ) ++ optionalMap(DockerValidation.key, dockerImageOption)

  protected def optionalMap[A](key: String, attrOption: Option[A]): Map[String, A] =
    attrOption.map(attr => Map(key -> attr)).getOrElse(Map.empty)
}
