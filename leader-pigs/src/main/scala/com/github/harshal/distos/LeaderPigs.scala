package com.github.harshal.distos

import com.codahale.logula.Logging
import org.apache.log4j.Level
import java.util.UUID
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.collection.mutable.LinkedHashMap
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
// An `AbstractPig` is an actor with a port and
// an empty sequence of actions to be performed on receipt
// of messages.
//
abstract class AbstractPig extends AbstractActor with Actor with Logging {
  //process id for this actor/pig
  val id:String

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
  case class SetNeighbors(ports: Seq[Int])
  case class DebugNeighbors()
}
trait Neighbors extends AbstractPig with Logging {
  this: AbstractPig =>
  override def actions = super.actions ++ Seq(action)
    
  import NeighborMessages._
  
  // The linked-map preserves the given neighbor ordering.
  @volatile var neighbors: LinkedHashMap[String, AbstractActor] = new LinkedHashMap()
    
  private val action: Any ~> Unit  = { 
    case n: SetNeighbors => { log.debug("" + port + " recieved neighbor list: " + n); setNeighbors(n); sender ! Ack }
    case    DebugNeighbors => { log.info("" + port + " Neighbors: " + neighbors.keys.mkString(",")) }
  }
  
  def setNeighbors(n: SetNeighbors): Unit = {
    neighbors = new LinkedHashMap[String,AbstractActor] ++ (n.ports.map(port =>
      id -> select(Node("localhost", port), Symbol(port.toString))
    ).toSeq)
  }
  
  // Send messages to all neighbors asynchronously.
  def flood(msg: Any) = for (n <- neighbors.values) n ! msg

}

//
// Lamport Clock
//
case class Clock() extends Ordered[Clock] {
  @volatile var _clockValue: Int = 0
  def tick(): Clock = { _clockValue += 1; this }
  def setMax(that: Clock) = _clockValue = scala.math.max(this._clockValue, that._clockValue)
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
  case class SetLeader(port: Int)
  case class WhoIsLeader()
  case class Leader(id: String)
}

//
// `RingBasedLeaderElection` is functionality that can be mixed into
// an `AbstractPig` which performs a ring-based leader election
// when triggered by an Election() message.
//
trait RingBasedLeaderElection extends AbstractPig {
  this: AbstractPig with Neighbors =>
    
  import RingBasedElectionMessages._
  import Constants.ELECTION_TIMEOUT
  
  var leader: Option[AbstractActor] = None
  
  def amLeader: Boolean = leader == Some(this)

  override def actions = super.actions ++ Seq(action)
    
  private val action: Any ~> Unit  = {
    case WhoIsLeader => leader match {
      case Some(l) => sender ! Leader(Some(neighbors.filter(_._2 == l).head._1))
      case None    => sender ! Leader(None)
    }
    case SetLeader(p) => {
      log.debug("%d setting leader to %d" format(port,p))
      leader = Some(if (p == id) this else neighbors(p))
      sender ! Ack
    }
    case e: Election => {
      sender ! Ack
      // If the message contains our id then we've gone around the circle.
      if (e.procIds.contains(id)) {
        log.info("Election %s finished: %s is the leader." format(e.id, e.max))
        // Send out the new leader to everyone.
        for ((p,n) <- neighbors) 
          n !? (200, SetLeader(e.max)) match { 
            case None => log.error("%d did not respond to Leader message" format p)
            case _    => ()
          }
        // Set the new leader for ourself too
        this ! SetLeader(e.max)
      } else {
        // Otherwise, we should pass it along, skipping
        // neighbors that don't respond within the timeout.
        val msg = e.copy(procIds = e.procIds ++ Seq(id))
        var done = false
        for ((p,n) <- neighbors) {
          if (!done) {
            (n !? (ELECTION_TIMEOUT, msg)) match {
              case Some(_) => {
                log.debug("Election from %d -> %d, succeeded..." format (port, p))
                done = true
              }
              case None => log.error("Election from %d -> %d, failed. Moving on..." format (port, p))
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
  case class Map(map: Seq[Option[Int]])
  case class Trajectory(x: Int, ttt:Int)
  case class Status()
  // TODO: currently, this isn't used
  case class StatusResponse(impacted: Boolean, moveTime: Option[Clock], hitTime: Option[Clock])
  case class BirdLanded(clock: Clock)
  case class GetPosition()
  case class SetPosition(x: Int)
  case class Position(x: Int)
  case class GetPort()
  case class Port(x: Int)
  
  // Pig => Game Engine
  case class WasHit(status: Boolean)
  
  type GameMap = Seq[Option[Int]]
  
  // Pig => Pig
  case class BirdApproaching(position: Int, clock: Clock)
}

trait PigGameLogic extends AbstractPig with Logging {
  this: AbstractPig with Neighbors with RingBasedLeaderElection with LamportClock =>
  override def actions = super.actions ++ Seq(action)

  import GameMessages._

  var currentPos:Int = -1
  var moveTime: Option[Clock] = None
  var hitTime: Option[Clock] = None
  var impacted: Boolean = false
  var gameMap: GameMessages.GameMap = null

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
   * Check if you are in an impacted zone and move
   * @param targetPos
   * @return
   */
  def moveIfRequired(targetPos: Int) = {
    if (isMoveRequired(targetPos))
      //if unable to move return true
      !move(currentPos+1)
    else 
      //if safe return false
      false
  }

  /**
   * To check if you need to move
   * @param targetPos
   * @return
   */
  def isMoveRequired(targetPos: Int) =
    (currentPos-1 == targetPos) || 
    (currentPos-2 == targetPos && isColumn(currentPos-1))

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

    case Map(map) => { gameMap = map; sender ! Ack }

    case Trajectory(targetPos, timeToTarget) => {
      if (amLeader) {
        flood(BirdApproaching(targetPos, clock.tick()))
        Thread.sleep(timeToTarget)
        flood(Status())
      }
    }

    case BirdApproaching(targetPos, incomingClock) => {
      
      clock.setMax(incomingClock)
      Util.simulateNetworkDelay(clock)
      
      // Move if required
      if (((targetPos == currentPos-1) && isColumn(currentPos-1)) ||
          currentPos == targetPos) {
        impacted = true
        val success = move()
        clock.tick()
        if (success)
          moveTime = Some(clock.copy())
      }
    }
    
    case BirdLanded(incomingClock) =>{
      clock.setMax(incomingClock)
      Util.simulateNetworkDelay(clock)
      clock.tick()
      hitTime = Some(clock.copy())
    }
    
    case Status() => {
      // TODO: checkIfHit returns true even if the pig didn't have to move.
      sender ! WasHit(checkIfHit(moveTime, hitTime))
    }
    
    case WasHit(isHit) => {
      //TODO
    }
    
    // Getters and Setters
    case GetPort()      => sender ! Port(port)
    case GetPosition()  => sender ! Position(currentPos)
    case SetPosition(x) => { currentPos = x; sender ! Ack }
  }
}


//
// Running it all.
//

class Pig(val port: Int) extends AbstractPig with Neighbors with RingBasedLeaderElection with LamportClock with PigGameLogic {
  val id  = UUID.randomUUID().toString.take(8)
}

class GameEngine(pigs: Seq[AbstractPig], worldSizeRatio: Int) extends Logging {
  import GameMessages._

  private val numPigs = pigs.size
  private val worldSize = worldSizeRatio * numPigs

  def generateMap(permutFn: Seq[Int] => Seq[Int] = rand.shuffle(_)): Array[Option[Int]] = {

    val world = Array.fill[Option[Int]](worldSize)(None)

    //Generate a random number of columns bounded by the number of pigs
    val numColumns = rand.nextInt(numPigs)

    //Generate a random permutation of the array indices
    val posVector: Seq[Int] = permutFn(0 until worldSize)

    val columnPos: Seq[Int] = posVector.takeRight(numColumns)

    for ((pig,pos) <- pigs.zip(posVector.take(numPigs)))
      world(pos) = Some(pig.port)

    for (pos <- columnPos)
      world(pos) = Some(COLUMN)

    world
  }

  def pickTarget = rand.nextInt(worldSize-1)

  def launch(
      targetPos: Int,
      leader   : AbstractPig,
      pigs     : Seq[AbstractPig],
      world    : Seq[Option[Int]],
      exit     : Boolean = true): Unit = {

    // Send out the game map to all pigs.
    for (pig <- pigs) {
      log.debug("Map sent waiting for ack..")
      pig !? Map(world)
      log.debug("Pig recieved Map and Ack'd")
    }

    println("""
              |  -----------------------
              |  |  Initial locations  |
              |  -----------------------
              |""".stripMargin)
    prettyPrintMap(world)

    // random time between 100 and 1000 ms
    val timeToTarget = rand.nextInt(450) + 550
    println("Time to target: " + timeToTarget)

    leader ! Trajectory(targetPos, timeToTarget)

//    // End the round
//    for (pig <- pigs)
//      pig !? EndGame()

//    Thread.sleep(1000)

//    val statuses = statusAll(pigs)

//    if (exit)
//      for (pig <- pigs)
//        pig !? Exit

//    statuses

  }
//
  def launch(leader:Pig) {
    val world = generateMap()
    val target = pickTarget
    launch(target, leader, pigs, world, exit = false)
//    prettyPrint(target, launch(target, pigs, world, exit = false), world)
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


object Main extends App {
  
  RemoteActor.classLoader = getClass().getClassLoader()
  
  !!!
  
  System.exit(0)
}

object Constants {
  val BASE_PORT = 10000
  val ELECTION_TIMEOUT = 300 //ms
}

object PigsRunner extends Logging {
  
  import Constants._
  import RingBasedElectionMessages._

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
  
  def setNeighborsInRingOrder(pigs: Seq[Pig], ids: Seq[Int]): Unit =
    for ((pig, neighbors) <- (pigs.zip(Stream.continually(ids).flatten.sliding(ids.size).map(_.drop(1).toArray.toSeq).toSeq)))
      pig !? NeighborMessages.SetNeighbors(neighbors)

  def main(args: Array[String]): Unit = {
    val numPigs = args(0).toInt
    val worldSizeRatio = args(1).toInt
    val (pigs, ports) = startPigs(numPigs)
    setNeighborsInRingOrder(pigs, ports)
    log.debug("Sending DebugNeighbors..")
    pigs.map(_ ! NeighborMessages.DebugNeighbors)
    
    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()
    
    Thread.sleep(500)
//    
//    log.debug("Killing the leader..")
//    pigs.last !? Exit
//    
//    log.debug("Initiating an election..")
//    pigs.head ! RingBasedElectionMessages.Election()
    
    // Find the leader
    val leader = pigs.find(_.amLeader) match {
      case Some(pig) => pig
      case None      => throw new Exception("Runner could not find the leader.")
    }
    
    //
    // Start the game.
    //
    val ge = new GameEngine(pigs, worldSizeRatio)
    
    log.info("generating the map..")
    ge.generateMap()
    
    log.info("launching...")
    ge.launch(leader)

    Thread.sleep(3000)
    log.debug("Sending exits..")
    pigs.map(_ !? (50, Exit))
 
    System.exit(0)
  }

}
