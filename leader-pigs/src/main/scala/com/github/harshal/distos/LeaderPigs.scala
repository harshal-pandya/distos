package com.github.harshal.distos

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.util.Random
import scala.collection.immutable.TreeMap
import Util._
import com.codahale.logula.Logging
import org.apache.log4j.Level

// Various, general utilities and conveniences used in the code.
object Util {
  
  // A message which indicates an empty reply.
  case class Ack
  
  // A type-constructor shortcut for PartialFunction
  type ~>[A, B] = PartialFunction[A, B]
  
  def !!! = throw new Error("Not yet implemnted!")
  
  val rand = new Random()

  class AnyOps[X](x: X) {
    def withEffect (f: X => Unit) : X = { f(x); x }
    def into[Y]    (f: X => Y)    : Y = f(x)
    def withFinally[Y](close: X => Unit)(f: X => Y): Y = try f(x) finally close(x)
    def mapNonNull [Y >: Null] (f: X => Y) : Y = if (x != null) f(x) else null
  }
  implicit def AnyOps[X](x: X) = new AnyOps(x)

  //
  // Setup logging
  //
  Logging.configure { log =>
    //log.registerWithJMX = true
  
    log.level = Level.DEBUG
    //log.loggers("com.myproject.weebits") = Level.OFF
  
    log.console.enabled = true
    log.console.threshold = Level.DEBUG
  
    //log.file.enabled = true
    //log.file.filename = "/var/log/myapp/myapp.log"
    //log.file.maxSize = 10 * 1024 // KB
    //log.file.retainedFiles = 5 // keep five old logs around
  }
}



// A stone column is simply a position on the map.
case class StoneColumn(position: Int)

//
// An `AbstractPig` is an actor with a port and
// an empty sequence of actions to be performed on receipt
// of messages.
//
abstract class AbstractPig extends AbstractActor with Actor with Logging {
  
  // The port on which this pig will run.
  val port: Int
  
  // The behaviors this pig has. 
  //[This is to be extended by various traits;
  // see `LeaderElection` for an example.]
  def actions: Seq[Any ~> Unit] = Seq(defaultActions)
  
  // Behaviors which all pigs should always have.
  val defaultActions: Any ~> Unit = {
    case Exit => { sender ! Ack; log.debug("" + port + " exiting."); exit() }
  }
  
  def act() {
    // Register as a RemoteActor
    alive(port)
    register(Symbol(port.toString), self)
    // Act on messages according to the behaviors
    // in `this.actions`.
    loop {
      react(actions.reduce(_ orElse _))
    }
  }
  
}

//
// Neighbors
//
object NeighborMessages {
  case class SetNeighbors(procIds: Seq[Int])
  case class DebugNeighbors()
}
trait Neighbors extends AbstractPig with Logging {
  this: AbstractPig =>
    
  import NeighborMessages._
  
  override def actions = super.actions ++ Seq(action)
  
  @volatile var neighbors: TreeMap[Int, AbstractActor] = TreeMap()
    
  private val action: Any ~> Unit  = { 
    case n: SetNeighbors => { log.debug("recieved neighbor list: " + n); setNeighbors(n); sender ! Ack }
    case    DebugNeighbors => { log.info("" + port + " Neighbors: " + neighbors.keys.mkString(",")) }
  }
  
  def setNeighbors(n: SetNeighbors): Unit = {
    neighbors = TreeMap(n.procIds.sorted.map(port => 
      port -> select(Node("localhost", port), Symbol(port.toString))
    ): _*).withEffect(x => log.debug("" + x))
  }

  def nextHighestNeighbor: (Int, AbstractActor) = !!!

}

//
// Leader Election
//

// Messages required by the `RingBasedLeaderElection` trait.
object RingBasedElectionMessages {
  
  // An Election message has an id and the process id's
  // of all the nodes it has been passed through.
  case class Election(procIds: Seq[Int]) {
    val id  = rand.nextInt;
    def max = procIds.max
  }
}

//
// `RingBasedLeaderElection` is functionality that can be mixed into
// an `AbstractPig` which performs a ring-based leader election
// when triggered.
//
trait RingBasedLeaderElection extends AbstractPig {
  this: AbstractPig with Neighbors =>
    
  import RingBasedElectionMessages._

  override def actions = super.actions ++ Seq(action)
    
  private val action: Any ~> Unit  = {
    case e: Election => handleElection(e)
  }
  
  def handleElection(e: Election) = !!!
  
}




//
// Running it all.
//

class Pig(val port: Int) extends AbstractPig with Neighbors with RingBasedLeaderElection

object Main extends App {
  
  RemoteActor.classLoader = getClass().getClassLoader()
  
  !!!
  
  System.exit(0)
}

object Constants {
  val BASE_PORT = 10000
}

object PigsRunner extends Logging {
  
  import Constants._

  RemoteActor.classLoader = getClass().getClassLoader()
  
  def startPigs(numPigs: Int): (Seq[Pig], Seq[Int]) = {
    val ports = (1 to numPigs).map(_ + BASE_PORT)
    val pigs  = (for (port <- ports) yield {
      log.info("Starting pig on port: " + port)
      val p = new Pig(port).withEffect(_.start())
      log.info("Started pig on port: " + port)
      p
    }).toSeq
    pigs -> ports
  }

  def main(args: Array[String]): Unit = {
    val numPigs = args(0).toInt
    val (pigs, ports) = startPigs(numPigs)
    log.debug("Sending SetNeighbors..")
    pigs.map(_ ! NeighborMessages.SetNeighbors(ports))
    log.debug("Sending DebugNeighbors..")
    pigs.map(_ ! NeighborMessages.DebugNeighbors)
    log.debug("Sending exits..")
    pigs.map(_ !? Exit)
    
    System.exit(0)
  }
  
}
