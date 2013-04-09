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
import scala.collection.mutable.LinkedHashMap

// Various, general utilities and conveniences used in the code.
object Util {
  
  // A message which indicates an empty reply.
  case class Ack
  
  // A type-constructor shortcut for PartialFunction
  type ~>[A, B] = PartialFunction[A, B]
  
  // Essentially a TODO that doesn't bother the type-checker.
  def !!! = throw new Error("Not yet implemnted!")
  
  val rand = new Random()

  // Convenience functions
  class AnyOps[X](x: X) {
    def withEffect (f: X => Unit) : X = { f(x); x }
    def into[Y]    (f: X => Y)    : Y = f(x)
    def withFinally[Y](close: X => Unit)(f: X => Y): Y = try f(x) finally close(x)
    def mapNonNull [Y >: Null] (f: X => Y) : Y = if (x != null) f(x) else null
  }
  implicit def AnyOps[X](x: X) = new AnyOps(x)

  // Setup logging
  Logging.configure { log =>
    log.level = Level.DEBUG
    log.console.enabled = true
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
    case Exit => { sender ! Ack; log.info("" + port + " exiting."); exit() }
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
  
  // The linked-map preserves the given neighbor ordering.
  @volatile var neighbors: LinkedHashMap[Int, AbstractActor] = new LinkedHashMap()
    
  private val action: Any ~> Unit  = { 
    case n: SetNeighbors => { log.debug("" + port + " recieved neighbor list: " + n); setNeighbors(n); sender ! Ack }
    case    DebugNeighbors => { log.info("" + port + " Neighbors: " + neighbors.keys.mkString(",")) }
  }
  
  def setNeighbors(n: SetNeighbors): Unit = {
    neighbors = new LinkedHashMap[Int,AbstractActor] ++ (n.procIds.map(port => 
      port -> select(Node("localhost", port), Symbol(port.toString))
    ).toSeq)
  }

}

//
// Leader Election
//

// Messages required by the `RingBasedLeaderElection` trait.
object RingBasedElectionMessages {
  
  // An Election message has an id and the process id's
  // of all the nodes it has been passed through.
  case class Election(procIds: Seq[Int] = Seq.empty) {
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
  import Constants.ELECTION_TIMEOUT
  
  @volatile var leader: Boolean = false

  override def actions = super.actions ++ Seq(action)
    
  private val action: Any ~> Unit  = {
    case e: Election => {
      sender ! Ack
      // If the message contains our port then we've gone around the circle.
      if (e.procIds.contains(port)) {
        log.info("Election %d finished: %d is the leader." format(e.id, e.max))
      } else {
        // Otherwise, we should pass it along, skipping
        // neighbors that don't responsd within the timeout.
        val msg = e.copy(procIds = e.procIds ++ Seq(port))
        var done = false
        for ((p,n) <- neighbors) {
          if (!done) {
            (n !? (ELECTION_TIMEOUT, msg)) match {
              case Some(_) => {
                log.debug("Election from %d -> %d, succeeded..."         format (port, p))
                done = true
              }
              case None    => log.error("Election from %d -> %d, failed. Moving on..." format (port, p))
            }
          }
        }
      }
    }
  }
  
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
  val ELECTION_TIMEOUT = 500 //ms
}

object PigsRunner extends Logging {
  
  import Constants._

  RemoteActor.classLoader = getClass().getClassLoader()
  
  def startPigs(numPigs: Int): (Seq[Pig], Seq[Int]) = {
    val ports = (1 to numPigs).map(_ + BASE_PORT).toIndexedSeq
    val pigs  = (for (port <- ports) yield
      new Pig(port)
        .withEffect(_.start())
        .withEffect(_ => log.info("Started pig on port: " + port))
    ).toSeq
    
    pigs -> ports
  }
  
  def setNeighborsInRingOrder(pigs: Seq[Pig], ports: Seq[Int]): Unit =
    for ((pig, neighbors) <- (pigs.zip(Stream.continually(ports).flatten.sliding(ports.size).map(_.drop(1).toArray.toSeq).toSeq)))
      pig !? NeighborMessages.SetNeighbors(neighbors)

  def main(args: Array[String]): Unit = {
    val numPigs = args(0).toInt
    val (pigs, ports) = startPigs(numPigs)
    setNeighborsInRingOrder(pigs, ports)
    log.debug("Sending DebugNeighbors..")
    pigs.map(_ ! NeighborMessages.DebugNeighbors)
    
    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()
    
    Thread.sleep(3000)
    
    log.debug("Killing the leader..")
    pigs.last !? Exit
    
    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()
    
    Thread.sleep(3000)
    log.debug("Sending exits..")
    pigs.map(_ !? (500, Exit))
 
    System.exit(0)
  }

}
