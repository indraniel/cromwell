package cromwell.backend.validation

import wdl4s.types.WdlStringType
import wdl4s.values.WdlString

import scalaz.Scalaz._

object DockerValidation extends RuntimeAttributesValidation[String] {
  override def key = RuntimeAttributesKeys.DockerKey

  override def coercion = Seq(WdlStringType)

  override protected def validateValue = {
    case WdlString(value) => value.successNel
  }

  override def staticDefaultOption = None

  override protected def failureMessage = s"Expecting $key runtime attribute to be a String"
}
