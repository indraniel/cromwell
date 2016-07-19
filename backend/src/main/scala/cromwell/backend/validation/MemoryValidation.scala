package cromwell.backend.validation

import cromwell.backend.MemorySize
import cromwell.core._
import wdl4s.parser.MemoryUnit
import wdl4s.types.{WdlIntegerType, WdlStringType}
import wdl4s.values.{WdlInteger, WdlString, WdlValue}

import scalaz.Scalaz._

object MemoryValidation {
  // TODO: Change RAK.MK to point here to MV.MK
  val MemoryKey = RuntimeAttributesKeys.MemoryKey
  val MemoryWrongAmountMsg = "Expecting %s runtime attribute value greater than 0 but got %s"
  val MemoryMissingFormatMsg = s"Expecting %s runtime attribute to be an Integer or String with format '8 GB'"
  val MemoryWrongFormatMsg =
    s"Expecting %s runtime attribute to be an Integer or String with format '8 GB'. Exception: %s"

  val validateMemory: PartialFunction[WdlValue, ErrorOr[MemorySize]] = {
    case WdlInteger(value) => MemoryValidation.validateMemoryInteger(value)
    case WdlString(value) => MemoryValidation.validateMemoryString(value)
  }

  def validateMemoryString(wdlString: WdlString): ErrorOr[MemorySize] = validateMemoryString(wdlString.value)

  def validateMemoryString(value: String): ErrorOr[MemorySize] = {
    MemorySize.parse(value) match {
      case scala.util.Success(memorySize: MemorySize) if memorySize.amount > 0 =>
        memorySize.to(MemoryUnit.GB).successNel
      case scala.util.Success(memorySize: MemorySize) =>
        MemoryWrongAmountMsg.format(MemoryKey, memorySize.amount.toString).failureNel
      case scala.util.Failure(throwable) =>
        MemoryWrongFormatMsg.format(MemoryKey, throwable.getMessage).failureNel
    }
  }

  def validateMemoryInteger(wdlInteger: WdlInteger): ErrorOr[MemorySize] = validateMemoryInteger(wdlInteger.value)

  def validateMemoryInteger(value: Int): ErrorOr[MemorySize] = {
    if (value <= 0)
      MemoryWrongAmountMsg.format(MemoryKey, value.toString).failureNel
    else
      MemorySize(value, MemoryUnit.Bytes).to(MemoryUnit.GB).successNel
  }

  lazy val Default = new RuntimeAttributesValidation[MemorySize] {
    override protected def validateValue = MemoryValidation.validateMemory

    override def key = MemoryValidation.MemoryKey

    override def coercion = Seq(WdlIntegerType, WdlStringType)

    override def failureMessage = MemoryMissingFormatMsg.format(MemoryKey)

    override def staticDefaultOption = None
  }

  lazy val Optional = Default.optional

  def withMemory(memorySize: MemorySize) = Default.withDefault(WdlInteger(memorySize.bytes.toInt))
}
