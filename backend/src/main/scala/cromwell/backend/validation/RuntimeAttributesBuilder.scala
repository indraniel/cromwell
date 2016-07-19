package cromwell.backend.validation

import cromwell.core._
import lenthall.exception.MessageAggregation
import org.slf4j.Logger
import wdl4s.types.WdlType
import wdl4s.values.WdlValue

import scalaz.{Failure, Success}

trait RuntimeAttributesBuilder[+RuntimeAttributesType] {
  protected def validate(values: Map[String, WdlValue]): ErrorOr[RuntimeAttributesType]

  def validations: Traversable[RuntimeAttributesValidation[_]]

  lazy val validationKeys = validations.map(_.key)

  lazy val coercionMap: Map[String, Traversable[WdlType]] = validations.map(attr => attr.key -> attr.coercion).toMap

  lazy val staticDefaults: Map[String, WdlValue] = (validations collect {
    case attr if attr.staticDefaultOption.isDefined => attr.key -> attr.staticDefaultOption.get
  }).toMap

  def build(attrs: Map[String, WdlValue], options: WorkflowOptions, logger: Logger): RuntimeAttributesType = {
    import RuntimeAttributesDefault._

    // Fail now if some workflow options are specified but can't be parsed correctly
    val defaultFromOptions = workflowOptionsDefault(options, coercionMap).get
    val withDefaultValues: Map[String, WdlValue] = withDefaults(attrs, List(defaultFromOptions, staticDefaults))

    RuntimeAttributesValidation.warnUnrecognized(withDefaultValues.keySet, validationKeys.toSet, logger)

    val runtimeAttributesErrorOr: ErrorOr[RuntimeAttributesType] = validate(withDefaultValues)
    runtimeAttributesErrorOr match {
      case Success(runtimeAttributes) => runtimeAttributes
      case Failure(nel) => throw new RuntimeException with MessageAggregation {
        override def exceptionContext: String = "Runtime attribute validation failed"

        override def errorMessages: Traversable[String] = nel.list
      }
    }
  }
}
