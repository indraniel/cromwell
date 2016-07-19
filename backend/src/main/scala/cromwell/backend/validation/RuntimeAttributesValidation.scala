package cromwell.backend.validation

import cromwell.backend.MemorySize
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.backend.wdl.OnlyPureFunctions
import cromwell.core._
import org.slf4j.Logger
import wdl4s.types.{WdlIntegerType, WdlType}
import wdl4s.values.{WdlValue, _}
import wdl4s.{NoLookup, WdlExpression}

import scala.util.{Failure, Success}
import scalaz.Scalaz._

object RuntimeAttributesValidation {

  def warnUnrecognized(actual: Set[String], expected: Set[String], logger: Logger) = {
    val unrecognized = actual.diff(expected).mkString(", ")
    if(unrecognized.nonEmpty) logger.warn(s"Unrecognized runtime attribute keys: $unrecognized")
  }

  def validateDocker(docker: Option[WdlValue], onMissingKey: => ErrorOr[Option[String]]): ErrorOr[Option[String]] = {
    docker match {
      case Some(WdlString(s)) => Some(s).successNel
      case None => onMissingKey
      case _ => s"Expecting $DockerKey runtime attribute to be a String".failureNel
    }
  }

  def validateFailOnStderr(value: Option[WdlValue], onMissingKey: => ErrorOr[Boolean]): ErrorOr[Boolean] = {
    value match {
      case Some(WdlBoolean(b)) => b.successNel
      case Some(WdlString(s)) if s.toLowerCase == "true" => true.successNel
      case Some(WdlString(s)) if s.toLowerCase == "false" => false.successNel
      case Some(_) => s"Expecting $FailOnStderrKey runtime attribute to be a Boolean or a String with values of 'true' or 'false'".failureNel
      case None => onMissingKey
    }
  }

  def validateContinueOnReturnCode(value: Option[WdlValue], onMissingKey: => ErrorOr[ContinueOnReturnCode]): ErrorOr[ContinueOnReturnCode] = {
    val failureWithMessage = s"Expecting $ContinueOnReturnCodeKey runtime attribute to be either a Boolean, a String 'true' or 'false', or an Array[Int]".failureNel
    value match {
      case Some(b: WdlBoolean) => ContinueOnReturnCodeFlag(b.value).successNel
      case Some(WdlString(s)) if s.toLowerCase == "true" => ContinueOnReturnCodeFlag(true).successNel
      case Some(WdlString(s)) if s.toLowerCase == "false" => ContinueOnReturnCodeFlag(false).successNel
      case Some(WdlInteger(i)) => ContinueOnReturnCodeSet(Set(i)).successNel
      case Some(WdlArray(wdlType, seq)) =>
        val nels: Seq[ErrorOr[Int]] = seq map validateInt
        val numFailures: Int = nels.count(_.isFailure)
        if (numFailures == 0) {
          val defaultReturnCodeNel = Set.empty[Int].successNel[String]
          nels.foldLeft(defaultReturnCodeNel)((acc, v) => (acc |@| v) { (a, v) => a + v }) map ContinueOnReturnCodeSet
        }
        else failureWithMessage
      case Some(_) => failureWithMessage
      case None => onMissingKey
    }
  }

  def validateInt(value: WdlValue): ErrorOr[Int] = {
    WdlIntegerType.coerceRawValue(value) match {
      case scala.util.Success(WdlInteger(i)) => i.intValue.successNel
      case _ => s"Could not coerce $value into an integer".failureNel
    }
  }

  def validateMemory(value: Option[WdlValue], onMissingKey: => ErrorOr[MemorySize]): ErrorOr[MemorySize] = {
    value match {
      case Some(i: WdlInteger) => parseMemoryInteger(i)
      case Some(s: WdlString) => parseMemoryString(s)
      case Some(_) =>
        String.format(MemoryValidation.MemoryWrongFormatMsg, MemoryKey, "Not supported WDL type value").failureNel
      case None => onMissingKey
    }
  }

  def parseMemoryString(s: WdlString): ErrorOr[MemorySize] = {
    MemoryValidation.validateMemoryString(s)
  }

  def parseMemoryInteger(i: WdlInteger): ErrorOr[MemorySize] = {
    MemoryValidation.validateMemoryInteger(i)
  }

  def validateCpu(cpu: Option[WdlValue], onMissingKey: => ErrorOr[Int]): ErrorOr[Int] = {
    val cpuValidation = cpu.map(validateInt).getOrElse(onMissingKey)
    cpuValidation match {
      case scalaz.Success(i) =>
        if (i <= 0)
          s"Expecting $CpuKey runtime attribute value greater than 0".failureNel
        else
          i.successNel
      case scalaz.Failure(f) =>
        s"Expecting $CpuKey runtime attribute to be an Integer".failureNel
    }
  }

  def withDefault[A](validation: RuntimeAttributesValidation[A],
                     default: WdlValue): RuntimeAttributesValidation[A] = {
    new RuntimeAttributesValidation[A] {
      override protected def validateValue = validation.validateValueInternal

      override def staticDefaultOption = Option(default)

      override def key = validation.key

      override def coercion = validation.coercion
    }
  }

  def optional[A](validation: RuntimeAttributesValidation[A]): OptionalRuntimeAttributesValidation[A] = {
    new OptionalRuntimeAttributesValidation[A] {
      override def key = validation.key

      override protected def validateOption = validation.validateValueInternal

      override protected def validateExpression = validation.validateExpressionInternal

      override def coercion = validation.coercion

      override protected def failureMessage = validation.failureMessageInternal
    }
  }
}

trait RuntimeAttributesValidation[A] {
  /**
    * Returns the key of the runtime attribute.
    *
    * @return The key of the runtime attribute.
    */
  def key: String

  /**
    * The WDL types that will be passed to `validate`, after the value is coerced from the first element found that
    * can coerce the type.
    *
    * @return traversable of wdl types
    */
  def coercion: Traversable[WdlType]

  /**
    * Returns the optional default value when no other is specified.
    *
    * @return the optional default value when no other is specified.
    */
  def staticDefaultOption: Option[WdlValue]

  /**
    * Returns message to return when a value is invalid.
    *
    * @return Message to return when a value is invalid.
    */
  protected def failureMessage: String = s"Expecting $key runtime attribute to be a type in $coercion"

  /**
    * Validates the wdl value.
    *
    * @return The validated value or an error, wrapped in a scalaz validation.
    */
  protected def validateValue: PartialFunction[WdlValue, ErrorOr[A]]

  /**
    * Returns the value for when there is no wdl value. By default returns an error.
    *
    * @return the value for when there is no wdl value.
    */
  protected def validateNone: ErrorOr[A] = failureWithMessage

  /**
    * Runs this validation on the value matching key.
    *
    * @param values The full set of values.
    * @return The error or valid value for this key.
    */
  def validate(values: Map[String, WdlValue]): ErrorOr[A] = {
    values.get(key) match {
      case Some(value) => validateValue.applyOrElse(value, (_: Any) => failureWithMessage)
      case None => validateNone
    }
  }

  protected def validateExpression: PartialFunction[WdlValue, Boolean] = {
    case wdlValue => coercion.exists(_ == wdlValue.wdlType)
  }

  def validateOptionalExpression(wdlExpressionMaybe: Option[WdlExpression]): Boolean = {
    wdlExpressionMaybe match {
      case None => staticDefaultOption.isDefined || validateNone.isSuccess
      case Some(wdlExpression) =>
        wdlExpression.evaluate(NoLookup, OnlyPureFunctions) match {
          case Success(wdlValue) => validateExpression.applyOrElse(wdlValue, (_: Any) => false)
          case Failure(throwable) =>
            throw new RuntimeException(s"Expression evaluation failed due to $throwable: $wdlExpression", throwable)
        }
    }
  }

  /**
    * Utility method to wrap the failureMessage in an ErrorOr.
    *
    * @return Wrapped failureMessage.
    */
  protected final lazy val failureWithMessage: ErrorOr[A] = failureMessage.failureNel

  final lazy val optional: OptionalRuntimeAttributesValidation[A] = RuntimeAttributesValidation.optional(this)

  final def withDefault(wdlValue: WdlValue) = RuntimeAttributesValidation.withDefault(this, wdlValue)

  private[validation] lazy val validateValueInternal: PartialFunction[WdlValue, ErrorOr[A]] = validateValue

  private[validation] lazy val validateExpressionInternal: PartialFunction[WdlValue, Boolean] = validateExpression

  private[validation] lazy val failureMessageInternal: String = failureMessage
}

trait OptionalRuntimeAttributesValidation[A] extends RuntimeAttributesValidation[Option[A]] {
  protected def validateOption: PartialFunction[WdlValue, ErrorOr[A]]

  override final protected lazy val validateValue = new PartialFunction[WdlValue, ErrorOr[Option[A]]] {
    override def isDefinedAt(wdlValue: WdlValue) = validateOption.isDefinedAt(wdlValue)

    override def apply(wdlValue: WdlValue) = validateOption.apply(wdlValue).map(Option.apply)
  }

  override final protected lazy val validateNone = None.successNel

  override def staticDefaultOption = None
}
