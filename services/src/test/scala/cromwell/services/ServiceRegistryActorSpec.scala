package cromwell.services


import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorInitializationException, ActorRef, OneForOneStrategy, Props}
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import cromwell.core.TestKitSuite
import cromwell.services.BarServiceActor.{ArbitraryBarMessage, SetProbe}
import cromwell.services.ServiceRegistryActor.{ServiceRegistryFailure, ServiceRegistryMessage}
import cromwell.services.ServiceRegistryActorSpec._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.language.postfixOps


abstract class EmptyActor extends Actor {
  override def receive: Receive = Actor.emptyBehavior
}

class FooServiceActor(config: Config, configp: Config) extends EmptyActor

class NoAppropriateConstructorServiceActor extends EmptyActor

object BarServiceActor {
  trait BarServiceMessage extends ServiceRegistryMessage {
    override def serviceName = "Bar"
  }
  final case class SetProbe(probe: TestProbe) extends BarServiceMessage
  case object ArbitraryBarMessage extends BarServiceMessage
}

class BarServiceActor(config: Config, configp: Config) extends Actor {
  var probe: TestProbe = _
  override def receive: Receive = {
    case SetProbe(p) => probe = p
    case m => probe.ref forward m
  }
}

object ServiceRegistryActorSpec {

  val ServicesBlockKey = "[SERVICES_BLOCK]"

  val ConfigurationTemplate =
    """
      |webservice {
      |  port = 8000
      |  interface = 0.0.0.0
      |  instance.name = "reference"
      |}
      |
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |  actor {
      |    default-dispatcher {
      |      fork-join-executor {
      |        # Number of threads = min(parallelism-factor * cpus, parallelism-max)
      |        # Below are the default values set by Akka, uncomment to tune these
      |
      |        #parallelism-factor = 3.0
      |        #parallelism-max = 64
      |      }
      |    }
      |
      |    deployment {
      |      /WorkflowManagerActor/WorkflowLogCopyRouter {
      |        router = round-robin-pool
      |        nr-of-instances = 10
      |      }
      |    }
      |  }
      |
      |  dispatchers {
      |    # A dispatcher for actors with slow/blocking behavior
      |    slow-actor-dispatcher {
      |      type = Dispatcher
      |      executor = "fork-join-executor"
      |      # Using the forkjoin defaults, this can be tuned if we wish
      |    }
      |  }
      |}
      |
      |spray.can {
      |  server {
      |    request-timeout = 40s
      |  }
      |  client {
      |    request-timeout = 40s
      |    connecting-timeout = 40s
      |  }
      |}
      |
      |backend {
      |  default = "Local"
      |  providers {
      |    Local {
      |      actor-factory = "cromwell.backend.impl.local.LocalBackendLifecycleActorFactory"
      |      config {
      |        root: "cromwell-executions"
      |
      |        filesystems {
      |          local {
      |            localization: [
      |              "hard-link", "soft-link", "copy"
      |            ]
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
      |
      |[SERVICES_BLOCK]
      |
      |database {
      |  config = main.hsqldb
      |
      |  main {
      |    hsqldb {
      |      db.url = "jdbc:hsqldb:mem:${slick.uniqueSchema};shutdown=false;hsqldb.tx=mvcc"
      |      db.driver = "org.hsqldb.jdbcDriver"
      |      db.connectionTimeout = 1000 // NOTE: 1000ms (the default) is ok for a small hsqldb, but often too short for mysql
      |      driver = "slick.driver.HsqldbDriver$"
      |    }
      |  }
      |
      |  test {
      |    mysql {
      |      db.url = "jdbc:mysql://localhost/cromwell_test"
      |      db.user = "travis"
      |      db.password = ""
      |      db.driver = "com.mysql.jdbc.Driver"
      |      db.connectionTimeout = 5000 // NOTE: The default 1000ms is often too short for production mysql use
      |      driver = "slick.driver.MySQLDriver$"
      |    }
      |  }
      |}
      |
    """.stripMargin

  private val ServiceNameKey = "[SERVICE_NAME]"
  private val ServiceClassKey = "[SERVICE_CLASS_NAME]"

  implicit class EnhancedServiceClass(val serviceClass: Class[_]) extends AnyVal {
    def serviceName = serviceClass.getSimpleName.replace("ServiceActor", "")

    def configEntry =
      s"""
         | $ServiceNameKey {
         |   class = "$ServiceClassKey"
         | }
      """.stripMargin.replace(ServiceNameKey, serviceClass.serviceName)
        .stripMargin.replace(ServiceClassKey, serviceClass.getCanonicalName)
  }

  val AwaitTimeout = 5 seconds
}

class ServiceRegistryActorSpec extends TestKitSuite("service-registry-actor-spec") with FlatSpecLike with Matchers with BeforeAndAfterAll {

  private def buildConfig(serviceClass: Class[_]): String = {
    val serviceEntriesKey = "[SERVICE_ENTRIES]"
    val template =
      s"""
         |  services {
         |    $serviceEntriesKey
         |  }
         |
      """.stripMargin

    val servicesBlock = template.replace(serviceEntriesKey, serviceClass.configEntry)
    ConfigurationTemplate.replace(ServicesBlockKey, servicesBlock)
  }

  private def buildProbeForInitializationException(config: Config): TestProbe = {
    val parentProbe = TestProbe()
    // c/p http://stackoverflow.com/questions/18619691/failing-a-scalatest-when-akka-actor-throws-exception-outside-of-the-test-thread/21892677
    system.actorOf(Props(new EmptyActor {
      context.actorOf(ServiceRegistryActor.props(config), s"ServiceRegistryActor-${UUID.randomUUID()}")
      override val supervisorStrategy = OneForOneStrategy() {
        case f => parentProbe.ref ! f; Stop
      }
    }))
    parentProbe
  }

  private def buildServiceRegistry(config: Config): ActorRef = {
    system.actorOf(ServiceRegistryActor.props(config), s"ServiceRegistryActor-${UUID.randomUUID()}")
  }

  behavior of "ServiceRegistryActorSpec"

  // The "die during construction" tests assert that an `ActorInitializationException` is sent to the Actor's supervisor
  // in a variety of circumstances which should probably trigger a Cromwell server shutdown.
  // These are unit tests and not integration tests, so there are no assertions here that the ServiceRegistryActor's
  // supervisor in the Cromwell actor hierarchy does anything sensible based upon receiving this exception.

  it should "die during construction if a config lacks a services block" in {
    val configString = buildConfig(classOf[FooServiceActor])
    val missingServices = configString.replace("  services ", "  shmervices")
    val probe = buildProbeForInitializationException(ConfigFactory.parseString(missingServices))
    probe.expectMsgPF(AwaitTimeout) {
      case e: ActorInitializationException =>
        e.getCause shouldBe a [ConfigException.Missing]
        e.getCause.getMessage shouldBe "No configuration setting found for key 'services'"
    }
  }

  it should "die during construction if a service class can't be found" in {
    val configString = buildConfig(classOf[FooServiceActor])
    val missingService = configString.replace("FooServiceActor", "FooWhoServiceActor")
    val probe = buildProbeForInitializationException(ConfigFactory.parseString(missingService))
    probe.expectMsgPF(AwaitTimeout) {
      case e: ActorInitializationException =>
        e.getCause shouldBe a [ClassNotFoundException]
        e.getCause.getMessage shouldBe "cromwell.services.FooWhoServiceActor"
    }
  }

  it should "die during construction if a service class lacks a proper constructor" in {
    val configString = buildConfig(classOf[NoAppropriateConstructorServiceActor])
    val probe = buildProbeForInitializationException(ConfigFactory.parseString(configString))
    probe.expectMsgPF(AwaitTimeout) {
      case e: ActorInitializationException =>
        e.getCause shouldBe an [IllegalArgumentException]
        e.getCause.getMessage should include("no matching constructor found on class cromwell.services.NoAppropriateConstructorServiceActor")
    }
  }

  it should "die during construction if a service configuration has no 'class' attribute" in {
    val configString = buildConfig(classOf[FooServiceActor])
    val missingService = configString.replace("class = \"cromwell.services.FooServiceActor\"", "")
    val probe = buildProbeForInitializationException(ConfigFactory.parseString(missingService))
    probe.expectMsgPF(AwaitTimeout) {
      case e: ActorInitializationException =>
        e.getCause shouldBe an [IllegalArgumentException]
        e.getCause.getMessage shouldBe "Invalid configuration for service Foo: missing 'class' definition"
    }
  }

  it should "respond with a failure for a message which is not a ServiceRegistryMessage" in {
    val configString = buildConfig(classOf[FooServiceActor])
    val service = buildServiceRegistry(ConfigFactory.parseString(configString))
    val probe = TestProbe()

    service.tell("This is a String, not an appropriate ServiceRegistryActor message", probe.ref)

    probe.expectMsgPF(AwaitTimeout) {
      case e: ServiceRegistryFailure =>
        e.serviceName should include("Message is not a ServiceRegistryMessage:")
    }
  }

  it should "reject a message for an unregistered service" in {
    // Configure only for the "Foo" service, send a "Bar" message.
    val configString = buildConfig(classOf[FooServiceActor])
    val service = buildServiceRegistry(ConfigFactory.parseString(configString))
    val probe = TestProbe()

    service.tell(ArbitraryBarMessage, probe.ref)

    probe.expectMsgPF(AwaitTimeout) {
      case e: ServiceRegistryFailure =>
        e.serviceName shouldBe "Bar"
    }
  }

  it should "forward a message to the appropriate service" in {
    val configString = buildConfig(classOf[BarServiceActor])
    val service = buildServiceRegistry(ConfigFactory.parseString(configString))

    val barProbe = TestProbe()
    service ! SetProbe(barProbe)
    service ! ArbitraryBarMessage

    barProbe.expectMsgPF(AwaitTimeout) {
      case ArbitraryBarMessage =>
      case x => fail("unexpected message: " + x)
    }
  }
}
