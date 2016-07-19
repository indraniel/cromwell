package cromwell.backend.validation

import wdl4s.types.{WdlBooleanType, WdlStringType}
import wdl4s.values.{WdlBoolean, WdlString}

import scalaz.Scalaz._

trait FailOnStderrValidation extends RuntimeAttributesValidation[Boolean] {
  override def key = RuntimeAttributesKeys.FailOnStderrKey

  override def coercion = Seq(WdlBooleanType, WdlStringType)

  override def staticDefaultOption = Option(WdlBoolean(false))

  override protected def failureMessage = {
    s"Expecting $key runtime attribute to be a Boolean or a String with values of 'true' or 'false'"
  }

  override protected def validateValue = {
      case WdlBoolean(value) => value.successNel
      case WdlString(value) if value.toLowerCase == "true" => true.successNel
      case WdlString(value) if value.toLowerCase == "false" => false.successNel
  }
}

object FailOnStderrValidation extends FailOnStderrValidation
