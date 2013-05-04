package com.github.harshal.distos

import org.apache.log4j.Level
import com.codahale.logula.Logging

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ConcurrentMap, LinkedHashMap}
import scala.collection.JavaConversions._
import scala.util.Random
import Util._

// Various, general utilities and conveniences used in the code.
object Util {

  // A message which indicates an empty reply.
  case class Ack

  // A type-constructor shortcut for PartialFunction
  type ~>[A, B] = PartialFunction[A, B]

  // Essentially a TODO that doesn't bother the type-checker.
  def !!! = throw new Error("Not yet implemented!")

  // Random to be used by everyone.
  val rand = new Random()

  // Convenience functions
  class AnyOps[X](x: X) {
    def withEffect (f: X => Unit) : X = { f(x); x }
  }
  implicit def AnyOps[X](x: X) = new AnyOps(x)

  // Setup logging
  Logging.configure { log =>
    log.level = Level.DEBUG
    log.console.enabled = true
  }

  // Clock Utils
  var NETWORK_DELAY: Int = 5
  def simulateNetworkDelay(clock: Clock): Unit = for (_ <- 0 to rand.nextInt(NETWORK_DELAY)) clock.tick()

  // The identifier of a stone column
  val COLUMN = -2
}

//
// An `AbstractNode` is an actor with a port and
// an empty sequence of actions to be performed on receipt
// of messages.
//
abstract class AbstractNode extends AbstractActor with Actor with Logging {
  //process id for this actor/pig
  val id:String = UUID.randomUUID().toString.take(8)

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
// Database
//
object DatabaseMessages {
  case class Update(id: String, dead: Boolean)
  case class Retrieve(id: String)
  case class Clear()
  case class GetSize()
  case class Size(size: Int)
}
trait Database extends AbstractNode with Logging {
  this: AbstractNode =>
  import DatabaseMessages._
  
  override def actions = super.actions ++ Seq(action)
  
  private val db: ConcurrentMap[String, Boolean] = new ConcurrentHashMap[String, Boolean]
  
  private val action: Any ~> Unit  = {
    case Update(id, dead) => sender ! db.update(id, dead)
    case Retrieve(id)     => sender ! db.get(id)
    case Clear()          => { db.clear(); sender ! Ack }
    case GetSize()        => sender ! Size(db.size)
  }
}
trait DatabaseConnection extends AbstractNode with Logging {
  this: AbstractNode =>
  lazy val db = { log.debug("" + port + " initiating db connection."); select(Node("localhost", Constants.DB_PORT), Symbol(Constants.DB_PORT.toString)) }
}

//
// Neighbors
//
object NeighborMessages {
  case class SetNeighbors(ports: Seq[Int])
  case class DebugNeighbors()
  case class GetId()
  case class Id(id: String)
}
trait Neighbors extends AbstractNode with Logging {
  this: AbstractNode =>
  override def actions = super.actions ++ Seq(action)

  import NeighborMessages._

  // The linked-map preserves the given neighbor ordering.
  @volatile var neighbors: Seq[AbstractActor] = Seq.empty
  @volatile var neighborsByPort: LinkedHashMap[Int,    AbstractActor] = new LinkedHashMap()
  @volatile var neighborsById:   LinkedHashMap[String, AbstractActor] = new LinkedHashMap()

  private val action: Any ~> Unit  = {
    case n: SetNeighbors => { log.debug("" + port + " received neighbor list: " + n); setNeighbors(n); sender ! Ack }
    case    DebugNeighbors => { log.info("" + port + " Neighbors: " + neighborsById.keys.mkString(",")) }
    case GetId() => sender ! Id(id)
  }

  def setNeighbors(n: SetNeighbors): Unit = {
    neighborsByPort = new LinkedHashMap[Int,AbstractActor] ++ (n.ports.map(port =>
      port -> select(Node("localhost", port), Symbol(port.toString))
    ).toSeq)
    neighbors = neighborsByPort.values.toSeq
    neighborsById = new LinkedHashMap[String, AbstractActor] ++ (neighbors.map(pig =>
      pig !? GetId() match {
        case Id(id) => Some(id -> pig)
        case _ => None
      }
    ).flatten)
  }

  // Send messages to all neighbors asynchronously and yourself.
  def flood(msg: Any) = {
    for (n <- neighbors) n ! msg
    this ! msg
  }

}

//
// Lamport Clock
//
case class Clock(initClockValue: Int = 0) extends Ordered[Clock] {
  @volatile var _clockValue: Int = initClockValue
  def tick(): Clock = { _clockValue += 1; this }
  def setMax(that: Clock): Clock = { _clockValue = scala.math.max(this._clockValue, that._clockValue); this }
  def copy() = { val c = new Clock; c._clockValue = _clockValue; c }
  def compare(that: Clock) = this._clockValue - that._clockValue
}

trait LamportClock { val clock: Clock = new Clock }

//
// Leader Election
//

// Messages required by the `RingBasedLeaderElection` trait.
object RingBasedElectionMessages {

  // An Election message has an id and the process id's
  // of all the nodes it has been passed through.
  // It is also used to initiate an Election.
  case class Election(procIds: Seq[String] = Seq.empty) {
    val id  = UUID.randomUUID().toString.take(8)
    def max = procIds.max
  }
  // A leader message informs the elected leader of the result.
  case class SetLeader(id: String)
  case class WhoIsLeader()
  case class LeaderId(id: Option[String])
}

//
// `RingBasedLeaderElection` is functionality that can be mixed into
// an `AbstractNode` which performs a ring-based leader election
// when triggered by an Election() message.
//
trait RingBasedLeaderElection extends AbstractNode {
  this: AbstractNode with Neighbors =>

  import RingBasedElectionMessages._
  import NeighborMessages._
  import Constants.ELECTION_TIMEOUT
  
  @volatile var leader: Option[AbstractActor] = None
  
  def amLeader = leader == Some(this)

  override def actions = super.actions ++ Seq(action)

  private val action: Any ~> Unit  = {
    case WhoIsLeader => actor {
      leader match {
        case Some(l) => sender ! LeaderId(if (amLeader) Some(id) else ((leader.get !? GetId()) match { case Id(id) => Some(id) }))
        case None    => sender ! LeaderId(None)
      }
    }
    case SetLeader(remoteId) => {
      log.debug("%s setting leader to %s" format(id, remoteId))
      leader = Some(if (remoteId == id) this else neighborsById(remoteId))
      sender ! Ack
    }
    case e: Election => {
      sender ! Ack
      // If we are the only pig then we're the leader
      if (e.procIds.isEmpty && neighbors.isEmpty)
        this ! SetLeader(id)
      // If the message contains our id then we've gone around the circle.
      else if (e.procIds.contains(id)) {
        log.info("Election %s finished: %s is the leader." format(e.id, e.max))
        // Send out the new leader to everyone.
        for ((nId,n) <- neighborsById) 
          n !? (ELECTION_TIMEOUT, SetLeader(e.max)) match { 
            case None => log.error("%s did not respond to Leader message" format nId)
            case _    => ()
          }
        // Set the new leader for ourself too
        this ! SetLeader(e.max)
      } else {
        // Otherwise, we should pass it along, skipping
        // neighbors that don't respond within the timeout.
        val msg = e.copy(procIds = e.procIds ++ Seq(id))
        var done = false
        for ((nId,n) <- neighborsById) {
          if (!done) {
            (n !? (ELECTION_TIMEOUT, msg)) match {
              case Some(_) => {
                log.debug("Election from %s -> %s, succeeded..." format (id, nId))
                done = true
              }
              case None => log.error("Election from %s -> %s, failed. Moving on..." format (id, nId))
            }
          }
        }
      }
    }
  }

}


//
// Game Logic
//
object GameMessages {

  // Game Engine => ()
  case class Start()

  // Game Engine => Pig
  case class SetMap(map: Seq[Option[Int]])
  case class Trajectory(x: Int, ttt: Clock)
  case class Status()
  // TODO: currently, this isn't used
  case class StatusResponse(impacted: Boolean, moveTime: Option[Clock], hitTime: Option[Clock])
  case class BirdLanded(clock: Clock)
  case class GetPosition()
  case class SetPosition(x: Int)
  case class Position(x: Int)
  case class GetPort()
  case class Port(x: Int)
  case class Statuses(map: mutable.HashMap[String,Boolean])
  // Pig => Game Engine
  case class WasHit(id:String,status: Boolean)

  case class GetStatusMap()

  type GameMap = Seq[Option[Int]]

  // Pig => Pig
  case class BirdApproaching(position: Int, clock: Clock)
}

trait PigGameLogic extends AbstractNode with Logging {
  this: AbstractNode with Neighbors with RingBasedLeaderElection with LamportClock =>
  override def actions = super.actions ++ Seq(action)

  import GameMessages._

  var currentPos:Int = -1
  var moveTime: Option[Clock] = None
  var hitTime: Option[Clock] = None
  var impacted: Boolean = false
  var gameMap: GameMessages.GameMap = null
  val statusMap = mutable.HashMap[String,Boolean]()

  /**
   * To check if next position is beyond the map boundary
   * @param pos
   * @return
   */
  def validPos(pos: Int) = !(pos < 0 || gameMap.size <= pos)

  /**
   * Check if this position is empty and available for moving into
   * @param pos
   * @return
   */
  def available(pos: Int) = validPos(pos) && gameMap(pos) == None

  /**
   * Find an empty spot next to yourself and move if possible
   * @return true if moved successfully
   */
  def move(): Boolean =  {
    if (available(currentPos - 1)){
      currentPos -= 1
      true
    }
    else if (available(currentPos + 1)){
      currentPos += 1
      true
    }
    else
      false
  }

  /**
   * Move to this position
   * @param pos
   * @return
   */
  def move(pos: Int): Boolean = {
    if (available(pos)) {
      currentPos = pos
      true
    }
    else false
  }

  /**
   * Check if there is a stone column present at this location
   * @param pos
   * @return
   */
  def isColumn(pos: Int) = validPos(pos) && (gameMap(pos) == Some(COLUMN))

  /**
   * Is this position not empty?
   * @param pos
   * @return
   */
  def isNotEmpty(pos: Int) = validPos(pos) && (gameMap(pos) != None)

  /**
   * compare the Lamport's clock for the move time and the hit time and
   * decide whether you are going to be hit
   * @param moveTime
   * @param hitTime
   * @return
   */
  def checkIfHit(moveTime: Option[Clock], hitTime: Option[Clock]): Boolean = {
    (moveTime, hitTime) match {
      case (Some(mtime), Some(htime)) => htime < mtime
      case (None,        Some(htime)) => true
      case _ => false.withEffect(_ => log.error("Hit time not set!!!!"))
    }
  }

  private val action: Any ~> Unit = {

    case SetMap(map) => { gameMap = map; sender ! Ack }

    case Trajectory(targetPos, timeToTarget) => {
      if (amLeader) {
        flood(BirdApproaching(targetPos, clock.tick()))
        flood(BirdLanded(timeToTarget))
        flood(Status())
      }
    }

    case BirdApproaching(targetPos, incomingClock) => {

      Util.simulateNetworkDelay(clock)
      clock.setMax(incomingClock)
      // Move if required
      if (((targetPos == currentPos-1) && isColumn  (currentPos-1)) ||
          ((targetPos == currentPos-2) && isColumn  (currentPos-1)) ||
          ((targetPos == currentPos-1) && isNotEmpty(currentPos-1)) ||
           (targetPos == currentPos)) {
        impacted = true
        val success = move()
        clock.tick()
        if (success)
          moveTime = Some(clock.copy())
      }
    }

    case BirdLanded(incomingClock) => hitTime = Some(clock.copy())
    case WasHit(id, isHit)         => statusMap.put(id, isHit)
    case Status()                  => sender ! WasHit(id, impacted && checkIfHit(moveTime, hitTime))

    // Getters and Setters
    case GetPort()      => sender ! Port(port)
    case GetPosition()  => sender ! Position(currentPos)
    case SetPosition(x) => { currentPos = x; sender ! Ack }
    case GetStatusMap() => sender ! Statuses(statusMap)
  }
}


//
// Running it all.
//

class DB(val port: Int) extends AbstractNode with Neighbors with Database

class Pig(val port: Int) extends AbstractNode with Neighbors with RingBasedLeaderElection with LamportClock with PigGameLogic with DatabaseConnection

class GameEngine(pigs: Seq[AbstractNode], worldSizeRatio: Double) extends Logging {
  import GameMessages._

  private val numPigs = pigs.size
  private val worldSize = math.floor(worldSizeRatio * numPigs).toInt

  def generateMap(permutFn: Seq[Int] => Seq[Int] = rand.shuffle(_)): Array[Option[Int]] = {

    val world = Array.fill[Option[Int]](worldSize)(None)

    //Generate a random number of columns bounded by the number of pigs
    val numColumns = rand.nextInt(numPigs)

    //Generate a random permutation of the array indices
    val posVector: Seq[Int] = permutFn(0 until worldSize)

    val columnPos: Seq[Int] = posVector.drop(numPigs).take(numColumns)

    for ((pig,pos) <- pigs.zip(posVector.take(numPigs))){
      pig !? SetPosition(pos)
      world(pos) = Some(pig.port)
    }

    for (pos <- columnPos)
      world(pos) = Some(COLUMN)

    world
  }

  def pickTarget = rand.nextInt(worldSize-1)

  def launch(
      targetPos: Int,
      leader   : AbstractNode,
      pigs     : Seq[AbstractNode],
      world    : Seq[Option[Int]],
      exit     : Boolean = true): Map[String, Boolean] = {

    // Send out the game map to all pigs.
    for (pig <- pigs) {
      log.debug("Map sent waiting for ack..")
      pig !? SetMap(world)
      log.debug("Pig recieved Map and Ack'd")
    }

    println("""
              |  -----------------------
              |  |  Initial locations  |
              |  -----------------------
              |""".stripMargin)
    prettyPrintMap(world)

    // random time between 3 and 8
    val timeToTarget = Clock(rand.nextInt(5) + 3)
    println("Time to target: " + timeToTarget._clockValue)

    leader ! Trajectory(targetPos, timeToTarget)

    Thread.sleep(2000)

    (leader !? GetStatusMap()) match { 
      case Statuses(map) => map.toMap
      case _ => Map()
    }
  }
  
  def launch(leader:Pig): Map[String, Boolean] = {
    val world = generateMap()
    val target = pickTarget
    launch(target, leader, pigs, world, exit = false)
    //prettyPrint(target, launch(target, pigs, world, exit = false), world)
  }

  def prettyPrintMap(world: Seq[Option[Int]]) {

    println(world.map { e => e match {
      case Some(x) => if (x == COLUMN) " | " else "   "
      case None => "   "
    }}.mkString(""))

    println(world.map { e => e match {
      case Some(x) => if (x == COLUMN) " | " else " @ "
      case None => " _ "
    }}.mkString(""))
    println("-" * world.size * 3)
  }

  def prettyPrint(target: Int, statuses: Seq[(Int, Boolean)], world: Seq[Option[Int]]) {

    println("""
              |  -----------------------
              |  |  Final locations    |
              |  -----------------------
              |
              |""".stripMargin)
    prettyPrintMap(world)

    println((" " * 3 * target)  + "XXX" + (" " * 3 * (world.size - target)) + "   <- TARGET")

    //    val newMap = world.map(x => x match {
    //      case Some(COLUMN) => COLUMN
    //      case Some(x)      => None
    //      case None         => None
    //    }).toArray

    //val undead = statuses.filterNot(_._2).map(_._1)

    //undead.foreach(x => newMap(x) = Some(1))

    //println((0 until newMap.size).map(i => if (!undead.contains(i)) " D " else "   ").mkString("") + "   <- DEAD?")

    //    println(statuses.mkString("\n"))
    //    println("target: " + target)

  }

}

object Constants {
  var DB_PORT = 9999 
  var BASE_PORT = 10000
  val ELECTION_TIMEOUT = 300 //ms
}

object PigsRunner extends Logging {

  import Constants._
  import RingBasedElectionMessages._

  RemoteActor.classLoader = getClass().getClassLoader()

  def startPigs(numPigs: Int): (Seq[Pig], Seq[Int]) = {
    val ports = (1 to numPigs).map(_ + BASE_PORT).toIndexedSeq
    val pigs  = ports.map(port => 
      new Pig(port)
        .withEffect(_.start())
        .withEffect(_ => log.info("Started pig on port: " + port))
    )

    pigs -> ports
  }
  
  def partitionPigs(pigs: Seq[Pig], ids: Seq[Int]): Seq[Seq[(Pig, Int)]] =
    pigs.zip(ids).grouped(math.ceil(pigs.size / 2.0).toInt).toSeq

  def setNeighborsInRingOrder(pigs: Seq[Pig], ids: Seq[Int]): Unit = {
    log.debug("Ring arranger pigs: %s" format (pigs.map(_.port).mkString(", "))) 
    log.debug("Ring arranger  ids: %s" format (ids.mkString(", "))) 
    for ((pig, neighbors) <- (pigs.zip(Stream.continually(ids).flatten.sliding(ids.size).map(_.drop(1).toArray.toSeq).toSeq)))
      pig !? NeighborMessages.SetNeighbors(neighbors)
  }
  
  def startDb(): DB =
    new DB(Constants.DB_PORT).withEffect { db => 
      db.start()
      log.info("Started pig on port: " + Constants.DB_PORT)
    }

  def main(args: Array[String]): Unit = {
    val numPigs = args(0).toInt
    val worldSizeRatio = args(1).toDouble
    val statuses = for (i<-1 to 5) yield {
      val (pigs, ports) = startPigs(numPigs)

      val part = partitionPigs(pigs, ports).withEffect(x => assert(x.size == 2))
      val (pigs1, ports1) = (part(0).map(_._1), part(0).map(_._2))
      val (pigs2, ports2) = (part(1).map(_._1), part(1).map(_._2))
 
      setNeighborsInRingOrder(pigs1, ports1)
      setNeighborsInRingOrder(pigs2, ports2)
 
      log.debug("Sending DebugNeighbors..")
      pigs.map(_ ! NeighborMessages.DebugNeighbors)

      log.debug("Initiating an election in set 1..")
      pigs1.head ! RingBasedElectionMessages.Election()
      log.debug("Initiating an election in set 2..")
      pigs2.head ! RingBasedElectionMessages.Election()
      Thread.sleep(1500)
      
      log.debug("Starting the database..")
      startDb()

      // Find the two leaders
      val leaders = pigs.filter(_.amLeader).withEffect(x => assert(x.size == 2))
      
      val leader = leaders(0)

      //
      // Start the game.
      //
      val ge = new GameEngine(pigs, worldSizeRatio)

      log.info("generating the map..")
      ge.generateMap()

      log.info("launching...")
      val status = ge.launch(leader)

      Thread.sleep(500)
      log.debug("Sending exits..")
      pigs.map(_ !? (50, Exit))
      Constants.BASE_PORT += 100
      status
    }
    
    val (_,exp) = stats(statuses)
    log.info("expected # dead pigs: " + exp)
    System.exit(0)
    
  }

  def stats(maps: Seq[Map[String,Boolean]]) = {
    val deadPigs = maps.map(_.filter(_._2).size).sum
    val numRounds = maps.length
    val expectation = deadPigs.toDouble / numRounds
    (deadPigs, expectation)
  }

}
