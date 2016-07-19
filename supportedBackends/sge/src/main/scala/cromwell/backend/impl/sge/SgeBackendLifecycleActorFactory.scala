package cromwell.backend.impl.sge

import akka.actor.Props
import cromwell.backend.BackendConfigurationDescriptor
import cromwell.backend.sharedfilesystem.{SharedFileSystemAsyncJobExecutionActorParams, SharedFileSystemBackendLifecycleActorFactory}

class SgeBackendLifecycleActorFactory(override val configurationDescriptor: BackendConfigurationDescriptor)
  extends SharedFileSystemBackendLifecycleActorFactory {
  override def asyncJobExecutionActorProps(params: SharedFileSystemAsyncJobExecutionActorParams) = {
    Props(new SgeAsyncJobExecutionActor(params))
  }

  override def runtimeAttributesBuilder = SgeRuntimeAttributesBuilder
}
