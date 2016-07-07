package cromwell.engine.workflow

import java.nio.file.Path
import java.util.UUID

import akka.actor.FSM.{CurrentState, Transition}
import akka.actor._
import akka.pattern.pipe
import better.files._
import cromwell.core.retry.SimpleExponentialBackoff
import cromwell.core.{WorkflowId, ExecutionStore => _, _}
import cromwell.engine._
import cromwell.engine.workflow.SingleWorkflowRunnerActor._
import cromwell.engine.workflow.WorkflowManagerActor.SubmitWorkflowCommand
import cromwell.services.MetadataServiceActor.{GetSingleWorkflowMetadataAction, GetStatus, WorkflowOutputs}
import cromwell.services.ServiceRegistryClient
import cromwell.util.PromiseActor._
import cromwell.webservice.CromwellApiHandler._
import cromwell.webservice.PerRequest.RequestComplete
import cromwell.webservice.metadata.MetadataBuilderActor
import spray.http.StatusCodes
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Try}

object SingleWorkflowRunnerActor {
  def props(source: WorkflowSourceFiles, metadataOutputFile: Option[Path], workflowManager: ActorRef): Props = {
    Props(classOf[SingleWorkflowRunnerActor], source, metadataOutputFile, workflowManager)
  }

  sealed trait RunnerMessage
  // The message to actually run the workflow is made explicit so the non-actor Main can `ask` this actor to do the
  // running and collect a result.
  case object RunWorkflow extends RunnerMessage
  private case object IssuePollRequest extends RunnerMessage
  private case object IssueReply extends RunnerMessage

  sealed trait RunnerState
  case object NotStarted extends RunnerState
  case object RunningWorkflow extends RunnerState
  case object RequestingOutputs extends RunnerState
  case object RequestingMetadata extends RunnerState
  case object Done extends RunnerState

  final case class RunnerData(replyTo: Option[ActorRef] = None,
                              id: Option[WorkflowId] = None,
                              terminalState: Option[WorkflowState] = None,
                              failures: Seq[Throwable] = Seq.empty) {

    def addFailure(message: String): RunnerData = addFailure(new Throwable(message))

    def addFailure(e: Throwable): RunnerData = this.copy(failures = e +: failures)
  }

  implicit class EnhancedJsObject(val jsObject: JsObject) extends AnyVal {
    def state: WorkflowState = WorkflowState.fromString(jsObject.fields.get("status").get.asInstanceOf[JsString].value)
  }

  private val Tag = "SingleWorkflowRunnerActor"
}

/**
 * Designed explicitly for the use case of the 'run' functionality in Main. This Actor will start a workflow,
 * print out the outputs when complete and then shut down the actor system. Note that multiple aspects of this
 * are sub-optimal for future use cases where one might want a single workflow being run.
 */
case class SingleWorkflowRunnerActor(source: WorkflowSourceFiles,
                                     metadataOutputPath: Option[Path],
                                     workflowManager: ActorRef)
  extends LoggingFSM[RunnerState, RunnerData] with CromwellActor with ServiceRegistryClient {

  import SingleWorkflowRunnerActor._
  import cromwell.MainSpecDebug._

  private val backoff = SimpleExponentialBackoff(1 second, 1 minute, 1.2)
  private implicit val system = context.system

  mainSpecDebug(s"SWRA starting") {
    startWith(NotStarted, RunnerData())
  }

  private def requestMetadata: State = {
    val metadataBuilder = context.actorOf(MetadataBuilderActor.props(serviceRegistryActor), s"MetadataRequest-Workflow-${stateData.id.get}")
    metadataBuilder ! GetSingleWorkflowMetadataAction(stateData.id.get, None, None)
    goto (RequestingMetadata)
  }

  private def schedulePollRequest(): Unit = {
    context.system.scheduler.scheduleOnce(backoff.backoffMillis.millis, self, IssuePollRequest)
  }

  private def requestStatus(): Unit = {
    // This requests status via the metadata service rather than instituting an FSM watch on the underlying workflow actor.
    // Cromwell's eventual consistency means it isn't safe to use an FSM transition to a terminal state as the signal for
    // when outputs or metadata have stabilized.
    val metadataBuilder = context.actorOf(MetadataBuilderActor.props(serviceRegistryActor), s"StatusRequest-Workflow-${stateData.id.get}-request-${UUID.randomUUID()}")
    metadataBuilder.askNoTimeout(GetStatus(stateData.id.get)) pipeTo self
  }

  private def issueReply: State = {
    self ! IssueReply
    goto (Done)
  }

  when (NotStarted) {
    case Event(RunWorkflow, data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} RunWorkflow") {
        log.info(s"$Tag: Launching workflow")
        val submitMessage = SubmitWorkflowCommand(source)
        workflowManager ! submitMessage
        goto(RunningWorkflow) using data.copy(replyTo = Option(sender()))
      }
  }

  when (RunningWorkflow) {
    case Event(WorkflowManagerSubmitSuccess(id), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} WorkflowManagerSubmitSuccess $id") {
        log.info(s"$Tag: Workflow submitted UUID($id)")
        schedulePollRequest()
        stay() using data.copy(id = Option(id))
      }
    case Event(IssuePollRequest, data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} IssuePollRequest") {
        requestStatus()
        stay()
      }
    case Event(RequestComplete((StatusCodes.OK, jsObject: JsObject)), data) if !jsObject.state.isTerminal =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} RequestComplete not terminal $data") {
        schedulePollRequest()
        stay()
      }
    case Event(RequestComplete((StatusCodes.OK, jsObject: JsObject)), data) if jsObject.state == WorkflowSucceeded =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} RequestComplete WorkflowSucceeded $data") {
        val metadataBuilder = context.actorOf(MetadataBuilderActor.props(serviceRegistryActor))
        metadataBuilder ! WorkflowOutputs(data.id.get)
        goto(RequestingOutputs) using data.copy(terminalState = Option(WorkflowSucceeded))
      }
    case Event(RequestComplete((StatusCodes.OK, jsObject: JsObject)), data) if jsObject.state == WorkflowFailed =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} RequestComplete WorkflowFailed $data") {
        val updatedData = data.copy(terminalState = Option(WorkflowFailed)).addFailure(s"Workflow ${data.id.get} transitioned to state Failed")
        // If there's an output path specified then request metadata, otherwise issue a reply to the original sender.
        val nextState = if (metadataOutputPath.isDefined) requestMetadata else issueReply
        nextState using updatedData
      }
  }

  when (RequestingOutputs) {
    case Event(RequestComplete((StatusCodes.OK, outputs: JsObject)), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} requesting outputs") {
        outputOutputs(outputs)
        if (metadataOutputPath.isDefined) requestMetadata else issueReply
      }
  }

  when (RequestingMetadata) {
    case Event(RequestComplete((StatusCodes.OK, metadata: JsObject)), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} requesting metadata") {
        outputMetadata(metadata)
        issueReply
      }
  }

  when (Done) {
    case Event(IssueReply, data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} done!") {
        data.terminalState foreach { state => log.info(s"$Tag workflow finished with status '$state'.") }
        data.failures foreach { e => log.error(e, e.getMessage) }

        val message = data.terminalState collect { case WorkflowSucceeded => () } getOrElse Status.Failure(data.failures.head)
        data.replyTo foreach { _ ! message }
        stay()
      }
  }

  private def failAndFinish(e: Throwable): State = {
    log.error(e, s"$Tag received Failure message: ${e.getMessage}")
    issueReply using stateData.addFailure(e)
  }

  whenUnhandled {
    // Handle failures for all WorkflowManagerFailureResponses generically.
    case Event(r: WorkflowManagerFailureResponse, data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} WorkflowManagerFailureResponse $r") {
        failAndFinish(r.failure)
      }
    case Event(Failure(e), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} Failure $e") {
        failAndFinish(e)
      }
    case Event(Status.Failure(e), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} Status.Failure $e") {
        failAndFinish(e)
      }
    case Event(RequestComplete((_, snap)), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} RequestComplete $snap") {
        failAndFinish(new RuntimeException(s"Unexpected API completion message: $snap"))
      }
    case Event(CurrentState(_, state), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} CurrentState\nstate = $state") {
        // ignore uninteresting current state and transition messages
        stay()
      }
    case Event(Transition(_, from, to), data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} Transition\nfrom = $from\nto  = $to") {
        // ignore uninteresting current state and transition messages
        stay()
      }
    case Event(m, data) =>
      mainSpecDebug(s"SWRA ${data.id.getOrElse("<none>")} Unhandled: $m") {
        log.warning(s"$Tag: received unexpected message: $m")
        stay()
      }
  }

  /**
    * Outputs the outputs to stdout, and then requests the metadata.
    */
  private def outputOutputs(outputs: JsObject): Unit = {
    println(outputs.prettyPrint)
  }

  private def outputMetadata(metadata: JsObject): Try[Unit] = {
    Try {
      val path = metadataOutputPath.get
      if (path.isDirectory) {
        log.error("Specified metadata path is a directory, should be a file: " + path)
      } else {
        log.info(s"$Tag writing metadata to $path")
        path.createIfNotExists().write(metadata.prettyPrint)
      }
    }
  }
}
