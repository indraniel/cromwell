package cromwell.backend.impl.sge

import java.nio.file.Path

import cromwell.backend.sharedfilesystem.{Command, SharedFileSystemAsyncJobExecutionActor, SharedFileSystemAsyncJobExecutionActorParams, SharedFileSystemJob}
import wdl4s.parser.MemoryUnit

class SgeAsyncJobExecutionActor(override val params: SharedFileSystemAsyncJobExecutionActorParams)
  extends SharedFileSystemAsyncJobExecutionActor[SgeJob, SgeRuntimeAttributes] {

  override def runtimeAttributesBuilder = SgeRuntimeAttributesBuilder

  override def processArgs: Command = {
    val sgeJobName = s"cromwell_${jobDescriptor.descriptor.id.shortString}_${jobDescriptor.call.unqualifiedName}"

    import lenthall.config.ScalaConfig._
    val config = configurationDescriptor.backendConfig
    val queueParam = config.getStringOption("queue").map(Seq("-q", _)).getOrElse(Seq.empty)
    val projectParam = config.getStringOption("project").map(Seq("-P", _)).getOrElse(Seq.empty)
    val memoryParamValue = for {
      memoryName <- config.getStringOption("memoryParam")
      memoryValue <- runtimeAttributes.memorySizeOption
    } yield s"$memoryName=${memoryValue.to(MemoryUnit.GB)}G"
    val memoryParam = memoryParamValue.map(Seq("-l", _)).getOrElse(Seq.empty)
    val argv = Seq(
      "qsub",
      "-terse",
      "-N", sgeJobName,
      "-V",
      "-b", "n",
      "-wd", jobPaths.callRoot.toAbsolutePath,
      "-o", jobPaths.stdout.toAbsolutePath,
      "-e", jobPaths.stderr.toAbsolutePath) ++
      queueParam ++
      projectParam ++
      memoryParam ++
      Seq(jobPaths.script.toAbsolutePath)

    new Command(argv)
  }

  def getJob(exitValue: Int, stdout: Path, stderr: Path): SgeJob = {
    import better.files._
    val jobId = stdout.contentAsString.stripLineEnd.toInt
    new SgeJob(jobId.toString)
  }

  override def killArgs(job: SgeJob): Command = {
    new Command(Seq("qdel", job.jobId))
  }
}

class SgeJob(override val jobId: String) extends SharedFileSystemJob
