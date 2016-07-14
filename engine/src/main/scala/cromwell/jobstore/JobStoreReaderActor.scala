package cromwell.jobstore

import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import cromwell.jobstore.JobStoreService.{JobComplete, JobNotComplete, JobStoreReadFailure, QueryJobCompletion}

import scala.util.{Failure, Success}

object JobStoreReaderActor {
  def props(database: JobStoreDatabase) = Props(new JobStoreReaderActor(database))
}

class JobStoreReaderActor(database: JobStoreDatabase) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive = LoggingReceive {
    case QueryJobCompletion(workflowId, key) =>
      val replyTo = sender()
      database.readJobResult(key.toJobStoreKey(workflowId)) onComplete {
        case Success(Some(result)) => replyTo ! JobComplete(key, result)
        case Success(None) => replyTo ! JobNotComplete(key)
        case Failure(t) => replyTo ! JobStoreReadFailure(key, t)
      }
    case unknownMessage => log.error(s"Unexpected message to JobStoreReader: $unknownMessage")
  }
}
