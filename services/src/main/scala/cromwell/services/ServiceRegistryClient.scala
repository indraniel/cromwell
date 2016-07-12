package cromwell.services

import akka.actor.ActorRef

/**
  * Small marker mixin noting that the implementing class has a reference to the ServiceRegistryActor
  */
// FIXME: this could go away
trait ServiceRegistryClient {
  val serviceRegistryActor: ActorRef
}
