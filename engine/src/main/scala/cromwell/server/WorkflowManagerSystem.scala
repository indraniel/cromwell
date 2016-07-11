
package cromwell.server

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import cromwell.engine.backend.{BackendConfiguration, CromwellBackends}
import cromwell.engine.workflow.{WorkflowManagerActor, WorkflowStoreActor}
import cromwell.services.ServiceRegistryActor
import org.slf4j.LoggerFactory

// FIXME: This whole thing should probably get a new name
trait WorkflowManagerSystem {
  protected def systemName = "cromwell-system"

  protected def newActorSystem(): ActorSystem = ActorSystem(systemName)
  val conf = ConfigFactory.load()
  val logger = LoggerFactory.getLogger(getClass.getName)
  implicit final lazy val actorSystem = newActorSystem()

  def shutdownActorSystem(): Unit = actorSystem.shutdown()

  CromwellBackends.initBackends(BackendConfiguration.AllBackendEntries, actorSystem)

  // FIXME: Add a higher level supervisor to guard these three
  lazy val serviceRegistryActor = actorSystem.actorOf(ServiceRegistryActor.props(ConfigFactory.load()), "ServiceRegistryActor")
  lazy val workflowStoreActor = actorSystem.actorOf(WorkflowStoreActor.props(serviceRegistryActor), "WorkflowStoreActor")
  lazy val workflowManagerActor = actorSystem.actorOf(WorkflowManagerActor.props(workflowStoreActor, serviceRegistryActor), "WorkflowManagerActor")
}
