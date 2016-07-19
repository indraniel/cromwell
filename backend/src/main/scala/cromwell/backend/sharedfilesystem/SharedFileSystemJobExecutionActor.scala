package cromwell.backend.sharedfilesystem

import java.nio.file.{Path, Paths}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import cromwell.backend.BackendJobExecutionActor.{AbortedResponse, BackendJobExecutionResponse}
import cromwell.backend.BackendLifecycleActor.AbortJobCommand
import cromwell.backend.async.AsyncBackendJobExecutionActor.{Execute, ExecutionMode}
import cromwell.backend.async.{AbortedExecutionHandle, AsyncBackendJobExecutionActor, ExecutionHandle, FailedNonRetryableExecutionHandle, NonRetryableExecution, SuccessfulExecutionHandle}
import cromwell.backend.io.{JobPaths, SharedFileSystem, SharedFsExpressionFunctions}
import cromwell.backend.{BackendConfigurationDescriptor, BackendInitializationData, BackendJobDescriptor, BackendJobDescriptorKey, BackendJobExecutionActor, ExecutionHash, OutputEvaluator}
import cromwell.core.logging.JobLogging
import cromwell.core.{JobOutputs, PathWriter}
import cromwell.services.CallMetadataKeys._
import cromwell.services.MetadataServiceActor.PutMetadataAction
import cromwell.services._
import wdl4s.WdlExpression
import wdl4s.util.TryUtil
import wdl4s.values.{WdlArray, WdlFile, WdlMap, WdlValue}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.sys.process._
import scala.util.{Failure, Success, Try}

class Command(val argv: Seq[Any]) {
  override def toString = argv.mkString("\"", "\" \"", "\"")
}

trait SharedFileSystemJob {
  def jobId: String
}

class SharedFileSystemJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                                        override val configurationDescriptor: BackendConfigurationDescriptor,
                                        asyncPropsCreator: Promise[BackendJobExecutionResponse] => Props)
  extends BackendJobExecutionActor {

  context.become(sharedStartup orElse super.receive)

  def sharedStartup: Receive = {
    case AbortJobCommand =>
      context.parent ! AbortedResponse(jobDescriptor.key)
      context.stop(self)
    case abortResponse: AbortedResponse =>
      context.parent ! abortResponse
      context.stop(self)
  }

  def sharedRunning(executor: ActorRef): Receive = {
    case AbortJobCommand =>
      executor ! AbortJobCommand
    case abortResponse: AbortedResponse =>
      context.parent ! abortResponse
      context.stop(self)
  }

  override def execute: Future[BackendJobExecutionResponse] = {
    // Still not sure why we don't wait for an Akka message instead of using Scala futures...
    val completionPromise = Promise[BackendJobExecutionResponse]()
    val executorRef = context.actorOf(asyncPropsCreator(completionPromise), "SharedFileSystemAsyncJobExecutionActor")
    context.become(sharedRunning(executorRef) orElse super.receive)
    executorRef ! Execute
    completionPromise.future
  }
}

trait SharedFileSystemAsyncJobExecutionActor[JobType <: SharedFileSystemJob,
RuntimeAttributesType <: SharedFileSystemRuntimeAttributes]
  extends Actor with ActorLogging with AsyncBackendJobExecutionActor with ServiceRegistryClient with JobLogging {

  case class SharedFileSystemPendingExecutionHandle(jobDescriptor: BackendJobDescriptor,
                                                    run: JobType) extends ExecutionHandle {
    override val isDone = false
    override val result = NonRetryableExecution(new IllegalStateException(
      "SharedFileSystemPendingExecutionHandle cannot yield a result"))
  }

  val SIGTERM = 143
  val SIGINT = 130

  import better.files._

  override protected implicit def ec = context.dispatcher

  val params: SharedFileSystemAsyncJobExecutionActorParams

  def processArgs: Command

  def getJob(exitValue: Int, stdout: Path, stderr: Path): JobType

  def killArgs(job: JobType): Command

  def toPath(path: WdlValue): WdlValue = path

  def configurationDescriptor: BackendConfigurationDescriptor = params.configurationDescriptor

  def backendInitializationDataOption: Option[BackendInitializationData] = params.backendInitializationDataOption

  override def jobDescriptor: BackendJobDescriptor = params.jobDescriptor

  override def completionPromise: Promise[BackendJobExecutionResponse] = params.completionPromise

  def toDockerPath(path: WdlValue): WdlValue = {
    path match {
      case file: WdlFile => WdlFile(jobPaths.toDockerPath(Paths.get(path.valueString)).toAbsolutePath.toString)
      case array: WdlArray => WdlArray(array.wdlType, array.value map toDockerPath)
      case map: WdlMap => WdlMap(map.wdlType, map.value mapValues toDockerPath)
      case wdlValue => wdlValue
    }
  }

  def supportsDocker = false

  def runsOnDocker = supportsDocker && runtimeAttributes.dockerImageOption.isDefined

  def scriptDir: Path = if (runsOnDocker) jobPaths.callDockerRoot else jobPaths.callRoot

  override def retryable = false

  lazy val workflowDescriptor = jobDescriptor.descriptor
  lazy val call = jobDescriptor.key.call
  lazy val jobPaths = new JobPaths(workflowDescriptor, configurationDescriptor.backendConfig, jobDescriptor.key,
    backendInitializationDataOption)
  lazy val callEngineFunction = SharedFsExpressionFunctions(jobPaths, SharedFileSystem.fileSystems)
  override lazy val workflowId = jobDescriptor.descriptor.id
  override lazy val jobTag = jobDescriptor.key.tag

  private val lookup = jobDescriptor.inputs.apply _

  private def evaluate(wdlExpression: WdlExpression) = wdlExpression.evaluate(lookup, callEngineFunction)

  def runtimeAttributesBuilder: SharedFileSystemRuntimeAttributesBuilder[RuntimeAttributesType]

  lazy val runtimeAttributes: RuntimeAttributesType = {
    val evaluateAttrs = call.task.runtimeAttributes.attrs mapValues evaluate
    // Fail the call if runtime attributes can't be evaluated
    val evaluatedAttributes = TryUtil.sequenceMap(evaluateAttrs, "Runtime attributes evaluation").get
    val builder = runtimeAttributesBuilder.withDockerSupport(supportsDocker)
    builder.build(evaluatedAttributes, jobDescriptor.descriptor.workflowOptions, jobLogger)
  }

  def sharedReceive(jobOption: Option[JobType]): Receive = LoggingReceive {
    case AbortJobCommand =>
      jobOption foreach tryKill
  }

  def instantiatedScript: String = {
    val pathTransformFunction: WdlValue => WdlValue = if (runsOnDocker) toDockerPath else toPath
    val tryCommand = sharedFileSystem.localizeInputs(jobPaths.callRoot,
      runsOnDocker, SharedFileSystem.fileSystems, jobDescriptor.inputs) flatMap { localizedInputs =>
      call.task.instantiateCommand(localizedInputs, callEngineFunction, pathTransformFunction)
    }
    tryCommand.get
  }

  override def executeOrRecover(mode: ExecutionMode)(implicit ec: ExecutionContext) = {
    Future.fromTry(Try {
      tellMetadata(startMetadataEvents)
      executeScript()
    })
  }

  private lazy val metadataJobKey = {
    val jobDescriptorKey: BackendJobDescriptorKey = jobDescriptor.key
    MetadataJobKey(jobDescriptorKey.call.fullyQualifiedName, jobDescriptorKey.index, jobDescriptorKey.attempt)
  }

  private def metadataKey(key: String) = MetadataKey(workflowId, Option(metadataJobKey), key)

  private def metadataEvent(key: String, value: Any) = MetadataEvent(metadataKey(key), MetadataValue(value))

  val runtimeAttributesEvents = runtimeAttributes.asMap map {
    case (key, value) =>
      metadataEvent(s"runtimeAttributes:$key", value)
  }

  def startMetadataEvents: Iterable[MetadataEvent] = runtimeAttributesEvents ++ List(
    metadataEvent(Stdout, jobPaths.stdout.toAbsolutePath),
    metadataEvent(Stderr, jobPaths.stderr.toAbsolutePath),
    // TODO: PBE: The REST endpoint toggles this value... how/where? Meanwhile, we read it decide to use the cache...
    metadataEvent("cache:allowResultReuse", true),
    metadataEvent(CallMetadataKeys.CallRoot, jobPaths.callRoot)
  )

  /**
    * Fire and forget info to the metadata service
    */
  def tellMetadata(events: Iterable[MetadataEvent]): Unit = {
    serviceRegistryActor ! PutMetadataAction(events)
  }

  def executeScript(): SharedFileSystemPendingExecutionHandle = {
    val script = instantiatedScript
    jobLogger.info(s"`$script`")
    scriptDir.createDirectories()
    writeScript(script, scriptDir)
    jobLogger.info(s"command: $processArgs")
    import cromwell.core.PathFactory.EnhancedPath
    val stdoutWriter = jobPaths.stdout.resolveSibling(s"${jobPaths.stdout.name}.submit").untailed
    val stderrWriter = jobPaths.stderr.resolveSibling(s"${jobPaths.stderr.name}.submit").untailed
    val argv = processArgs.argv.map(_.toString)
    val proc = argv.run(ProcessLogger(stdoutWriter.writeWithNewline, stderrWriter.writeWithNewline))
    val run = getJob(proc, stdoutWriter, stderrWriter)
    tellMetadata(Seq(metadataEvent("jobId", run.jobId)))
    SharedFileSystemPendingExecutionHandle(jobDescriptor, run)
  }

  def getJob(process: Process, stdoutWriter: PathWriter, stderrWriter: PathWriter): JobType = {
    val exitValue = process.exitValue()
    import cromwell.core.PathFactory.FlushingAndClosingWriter
    stdoutWriter.writer.flushAndClose()
    stderrWriter.writer.flushAndClose()
    getJob(exitValue, stdoutWriter.path, stdoutWriter.path)
  }

  def tryKill(job: JobType): Unit = {
    import better.files._
    import cromwell.core.PathFactory._
    val returnCodeTmp = jobPaths.returnCode.resolveSibling(s"${jobPaths.returnCode.getFileName}.kill")
    val stdoutWriter = jobPaths.stdout.resolveSibling(s"${jobPaths.stdout.name}.kill").untailed
    val stderrWriter = jobPaths.stderr.resolveSibling(s"${jobPaths.stderr.name}.kill").untailed
    returnCodeTmp.write(s"$SIGTERM\n")
    returnCodeTmp.moveTo(jobPaths.returnCode)
    val argv = killArgs(job).argv.map(_.toString)
    val proc = argv.run(ProcessLogger(stdoutWriter.writeWithNewline, stderrWriter.writeWithNewline))
    proc.exitValue()
    stdoutWriter.writer.flushAndClose()
    stderrWriter.writer.flushAndClose()
  }

  def processReturnCode()(implicit ec: ExecutionContext): Future[ExecutionHandle] = {
    val returnCodeTry = Try(jobPaths.returnCode.contentAsString.stripLineEnd.toInt)

    lazy val badReturnCodeMessage =
      s"Call ${call.fullyQualifiedName}, Workflow ${workflowDescriptor.id}: " +
        s"return code was ${returnCodeTry.getOrElse("(none)")}"

    lazy val badReturnCodeResponse = Future.successful(
      FailedNonRetryableExecutionHandle(new Exception(badReturnCodeMessage), returnCodeTry.toOption))

    lazy val abortResponse = Future.successful(AbortedExecutionHandle)

    def processSuccess(returnCode: Int) = {
      val successfulFuture = for {
        outputs <- Future.fromTry(processOutputs())
        hash <- ExecutionHash.completelyRandomExecutionHash
      } yield SuccessfulExecutionHandle(outputs, returnCode, hash, None)

      successfulFuture recover {
        case failed: Throwable =>
          FailedNonRetryableExecutionHandle(failed, Option(returnCode))
      }
    }

    def stopFor(returnCode: Int) = !runtimeAttributes.continueOnReturnCode.continueFor(returnCode)

    def failForStderr = runtimeAttributes.failOnStderr && jobPaths.stderr.size > 0

    returnCodeTry match {
      case Success(SIGTERM) => abortResponse // Special case to check for SIGTERM exit code - implying abort
      case Success(SIGINT) => abortResponse // Special case to check for SIGINT exit code - implying abort
      case Success(returnCode) if stopFor(returnCode) => badReturnCodeResponse
      case Success(returnCode) if failForStderr => badReturnCodeResponse
      case Success(returnCode) => processSuccess(returnCode)
      case Failure(e) => badReturnCodeResponse
    }
  }

  override def poll(previous: ExecutionHandle)(implicit ec: ExecutionContext) = {
    previous match {
      case handle: SharedFileSystemPendingExecutionHandle =>
        val runId = handle.run
        jobLogger.debug(s"Polling Job $runId")
        jobPaths.returnCode.exists match {
          case true => processReturnCode()
          case false =>
            jobLogger.info(s"'${jobPaths.returnCode}' file does not exist yet")
            Future.successful(previous)
        }
      case failed: FailedNonRetryableExecutionHandle => Future.successful(failed)
      case successful: SuccessfulExecutionHandle => Future.successful(successful)
      case bad => Future.failed(new IllegalArgumentException(s"Unexpected execution handle: $bad"))
    }
  }

  /**
    * Writes the script file containing the user's command from the WDL as well
    * as some extra shell code for monitoring jobs
    */
  private def writeScript(instantiatedCommand: String, containerRoot: Path) = {
    jobPaths.script.write(
      s"""#!/bin/sh
          |cd $containerRoot
          |$instantiatedCommand
          |echo $$? > rc
          |""".stripMargin)
  }

  private def processOutputs(): Try[JobOutputs] = {
    OutputEvaluator.evaluateOutputs(jobDescriptor, callEngineFunction, sharedFileSystem.outputMapper(jobPaths))
  }

  private val sharedFileSystem = new SharedFileSystem {
    override def sharedFsConfig = configurationDescriptor.backendConfig.getConfig("filesystems.local")
  }
}
