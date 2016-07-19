package cromwell.server

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorInitializationException, ActorRef, OneForOneStrategy, Props}
import akka.event.Logging
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory
import cromwell.engine.workflow.lifecycle.CopyWorkflowLogsActor
import cromwell.engine.workflow.{WorkflowManagerActor, WorkflowStoreActor}
import cromwell.services.ServiceRegistryActor

/**
  * An actor which is purely used as a supervisor for the rest of Cromwell, allowing us to have more fine grained
  * control on top level supervision, etc.
  *
  * For now there are only two entries into Cromwell - either using SingleWorkflowRunnerActor or CromwellServerActor.
  * This is intended to be mixed in with those entry points.
  *
  * If any of the actors created by CromwellRootActor fail to initialize the ActorSystem will die, which means that
  * Cromwell will fail to start in a bad state regardless of the entry point.
  */
 trait CromwellRootActor extends Actor {
  private val logger = Logging(context.system, this)

   lazy val serviceRegistryActor = {
     println("DAFUQ?")
     context.actorOf(ServiceRegistryActor.props(ConfigFactory.load()), "ServiceRegistryActor")
   }

   val workflowLogCopyRouter: ActorRef = context.actorOf(RoundRobinPool(10) // FIXME: get the config stuff here
      .withSupervisorStrategy(CopyWorkflowLogsActor.strategy)
      .props(CopyWorkflowLogsActor.props(serviceRegistryActor).withDispatcher("akka.dispatchers.slow-actor-dispatcher")),
      "WorkflowLogCopyRouter")

   val workflowStoreActor = context.actorOf(WorkflowStoreActor.props(serviceRegistryActor), "WorkflowStoreActor")
   val workflowManagerActor = context.actorOf(WorkflowManagerActor.props(workflowStoreActor, serviceRegistryActor, workflowLogCopyRouter), "WorkflowManagerActor")

  override def receive = {
    case _ => logger.error("CromwellRootActor is receiving a message. It prefers to be left alone!")
  }

  /**
    * Validate that all of the direct children actors were successfully created, otherwise error out the initialization
    * of Cromwell by passing a Throwable to the guardian.
    */
  override val supervisorStrategy = OneForOneStrategy() {
    case aie: ActorInitializationException => throw new Throwable(s"Unable to create actor for ActorRef ${aie.getActor}", aie.getCause)
    case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
  }
}
