package cromwell.backend.validation

import cromwell.backend.validation.RuntimeAttributesValidation._
import cromwell.core._
import wdl4s.types.{WdlArrayType, WdlStringType}
import wdl4s.values.{WdlArray, WdlBoolean, WdlInteger, WdlString}

import scalaz.Scalaz._

trait ContinueOnReturnCodeValidation extends RuntimeAttributesValidation[ContinueOnReturnCode] {
  override def key = RuntimeAttributesKeys.ContinueOnReturnCodeKey

  override def staticDefaultOption = Option(WdlInteger(0))

  override def coercion = ContinueOnReturnCode.validWdlTypes

  override protected def failureMessage = {
    s"Expecting $key runtime attribute to be either a Boolean, a String 'true' or 'false', or an Array[Int]"
  }

  override def validateValue = {
    case WdlBoolean(value) => ContinueOnReturnCodeFlag(value).successNel
    case WdlString(value) if value.toLowerCase == "true" => ContinueOnReturnCodeFlag(true).successNel
    case WdlString(value) if value.toLowerCase == "false" => ContinueOnReturnCodeFlag(false).successNel
    case WdlInteger(value) => ContinueOnReturnCodeSet(Set(value)).successNel
    case WdlArray(wdlType, seq) =>
      val nels: Seq[ErrorOr[Int]] = seq map validateInt
      val numFailures: Int = nels.count(_.isFailure)
      if (numFailures == 0) {
        val defaultReturnCodeNel = Set.empty[Int].successNel[String]
        nels.foldLeft(defaultReturnCodeNel)((acc, v) => (acc |@| v) { (a, v) => a + v }) map ContinueOnReturnCodeSet
      } else {
        failureWithMessage
      }
  }

  override def validateExpression = {
    case _: WdlBoolean => true
    case WdlString(value) if value.toLowerCase == "true" => true
    case WdlString(value) if value.toLowerCase == "false" => true
    case _: WdlInteger => true
    case WdlArray(WdlArrayType(WdlStringType), elements) => elements.forall(validateInt(_).isSuccess)
  }
}

object ContinueOnReturnCodeValidation extends ContinueOnReturnCodeValidation
