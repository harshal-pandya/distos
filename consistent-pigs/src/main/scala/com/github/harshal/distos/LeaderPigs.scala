package com.github.harshal.distos

import org.apache.log4j.Level
import com.codahale.logula.Logging

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import collection.mutable
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
  case class Register(port: Int)
  case class Update(port:Int,buffer:mutable.HashMap[Int,Boolean])
  case class Retrieve(id: String)
  case class Clear()
  case class GetSize()
  case class Size(size: Int)
  case class GetDBCopy()
  case class DBCopy(map:mutable.HashMap[Int,Boolean])
}
trait Database extends AbstractNode with Logging {
  this: AbstractNode
  import DatabaseMessages._
  import RingBasedElectionMessages._

  def initMap(ports: Seq[Int]) = ports.foreach(p => db.put(p, true))

  override def actions = super.actions ++ Seq(action)
  
  @volatile var leaders = Set.empty[AbstractActor]
  
  val db: ConcurrentMap[Int, Boolean] = new ConcurrentHashMap[Int, Boolean]
  
  private val action: Any ~> Unit  = {
    case Update(port, buffer) => {
      log.debug("Starting database update...")
      log.debug("Updating database with values:\n%s" format buffer.mkString("\n"))
      buffer.foreach { case (k,v) => db.update(k,v) }
      log.debug("Attempting to contact second leader on port: " + port)
      val other = select(Node("localhost", port), Symbol(port.toString))
      (other !? WhoIsLeader()) match { case LeaderId(id) => log.debug("Second leader is: " + id) }
      log.debug("Successful contact with secondary leader.")
      other !? GameMessages.DatabasePush(buffer)
      sender ! Ack
    }
    case Retrieve(id)     => sender ! db.get(id)
    case Clear()          => { db.clear(); sender ! Ack }
    case GetSize()        => sender ! Size(db.size)
    case Register(port)   => { 
      leaders = leaders + select(Node("localhost", port), Symbol(port.toString))
      log.debug("Added " + port + " as leader.")
      sender ! Ack
    }
    case GetDBCopy() => {
      log.debug("Fetching cache from DB..")
      sender ! DBCopy(mutable.HashMap[Int,Boolean](db.toSeq:_*))
    }
  }
}

trait DatabaseConnection extends AbstractNode with Logging {
  this: AbstractNode =>
  lazy val db = { log.debug("" + port + " initiating db connection."); select(Node("localhost", Constants.DB_PORT), Symbol(Constants.DB_PORT.toString)) }
}

class LocalCache(private val cache:mutable.HashMap[String,Boolean]){
  def apply(id:String,dead:Boolean) = cache.update(id,dead)
  def apply(id:String) = cache(id)
}



//
// Neighbors
//
object NeighborMessages {
  case class SetNeighbors(ports: Seq[Int])
  case class UpdateNeighbors(ports: Seq[Int])
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
  @volatile var neighborsByPort  = new LinkedHashMap[Int   , AbstractActor]()
  @volatile var neighborsById    = new LinkedHashMap[String, AbstractActor]()

  private val action: Any ~> Unit  = {
    case n: SetNeighbors    => { log.debug("" + port + " received neighbor list: " + n)       ; setNeighbors(n)   ; sender ! Ack }
    case n: UpdateNeighbors => { log.debug("" + port + " received neighbor update list: " + n); updateNeighbors(n); sender ! Ack }
    case    DebugNeighbors  => { log.info ("" + port + " Neighbors: " + neighborsById.keys.mkString(",")) }
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
  
  def updateNeighbors(n :UpdateNeighbors) : Unit = {
    neighborsByPort = neighborsByPort ++ (n.ports.map(port =>
      port -> select(Node("localhost", port), Symbol(port.toString))
    ).toSeq)
    neighbors = neighborsByPort.values.toSeq
    // XXX: not updating neighborsById. This shouldn't be a problem though.
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

object SecondaryLeaderMessages {
  case class SecondaryLeader(port: Int)
}
trait SecondaryLeader extends AbstractNode {
  this: AbstractNode =>
  import SecondaryLeaderMessages._
  override def actions = super.actions ++ Seq(action)
  
  @volatile var secondaryLeader: Option[AbstractActor] = None
  @volatile var secondaryLeaderPort: Option[Int]       = None
  
  private val action: Any ~> Unit  = {
    case SecondaryLeader(port) => {
      secondaryLeader = Some(select(Node("localhost", port), Symbol(port.toString)))
      secondaryLeaderPort = Some(port)
      log.debug("Leader (%s) set secondary leader to: %s" format (this.port, port))
      sender ! Ack
    }
  }
}

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
  this: AbstractNode with Neighbors with DatabaseConnection =>

  import RingBasedElectionMessages._
  import NeighborMessages._
  import Constants.ELECTION_TIMEOUT
  
  @volatile var leader: Option[AbstractActor] = None
  
  def amLeader = leader == Some(this)

  override def actions = super.actions ++ Seq(action)

  private val action: Any ~> Unit  = {
    case WhoIsLeader() => actor {
      leader match {
        case Some(l) => sender ! LeaderId(if (amLeader) Some(id) else ((leader.get !? GetId()) match { case Id(id) => Some(id) }))
        case None    => sender ! LeaderId(None)
      }
    }
    case SetLeader(remoteId) => {
      log.debug("%s setting leader to %s" format(id, remoteId))
      leader = Some(if (remoteId == id) this else neighborsById(remoteId))
      // The leader holds the responsibility of connecting to the database.
      if (amLeader) db !? (Constants.DB_TIMEOUT, DatabaseMessages.Register(port))
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
// Fault Tolerance
//

object FaultToleranceMessages {
  case class Sleep()
  case class AreYouAwake()
  case class CheckIfAwake()
}
trait FaultTolerance extends AbstractNode {
  this: AbstractNode with Neighbors with SecondaryLeader with PigGameLogic =>
  override def actions = super.actions ++ Seq(action)

  import FaultToleranceMessages._

  @volatile var sleeping = false

  private val action: Any ~> Unit  = {
    case Sleep()       => { log.info("%d sleeping." format port); sleeping = true; sender ! Ack }
    case AreYouAwake() => { if (!sleeping) sender ! Ack }
    case CheckIfAwake() => actor {
      if(!sleeping) {
        secondaryLeader.get !? (Constants.CHECK_AWAKE_TIMEOUT, AreYouAwake()) match {
          case None => {
            log.error("%s did not respond to Leader message" format secondaryLeaderPort)
            log.debug("%s: Synchronously flooding new neighbors" format port)
            for (n <- neighbors ++ Seq(this))
              n !? NeighborMessages.UpdateNeighbors(cache.keys.toSeq)
            log.debug("%s: Done updating all minions." format port)
          }
          case _ =>  log.debug("Secondary leader is awake. %d continuing as normal." format port)
        }
      }
      log.debug("%s: Ack'ing CheckIfAwake." format port)
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
  // Pig => Game Engine
  case class WasHit(port:Int,status: Boolean)

  case class GetStatusMap()
  case class Statuses(map: mutable.HashMap[Int,Boolean])

  case class DatabasePush(updates:mutable.HashMap[Int,Boolean])

  type GameMap = Seq[Option[Int]]

  // Pig => Pig
  case class BirdApproaching(position: Int, clock: Clock)
}

trait PigGameLogic extends AbstractNode with Logging {
  this: AbstractNode with Neighbors with RingBasedLeaderElection with LamportClock with DatabaseConnection with SecondaryLeader with FaultTolerance =>
  override def actions = super.actions ++ Seq(action)

  import GameMessages._
  import DatabaseMessages._
  @volatile var currentPos:Int = -1
  @volatile var moveTime: Option[Clock] = None
  @volatile var hitTime: Option[Clock] = None
  @volatile var impacted: Boolean = false
  @volatile var dead = false
  @volatile var gameMap: GameMessages.GameMap = null
  @volatile var buffer:mutable.HashMap[Int,Boolean] = mutable.HashMap[Int,Boolean]()
  lazy val cache = (db !? DatabaseMessages.GetDBCopy()) match {
    case DBCopy(map) => map
    case _ => !!!
  }
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
  
 def commit(buffer: mutable.HashMap[Int,Boolean]) =
    buffer.foreach { case (k,v) => cache.update(k,v) }

  private val action: Any ~> Unit = {

    case SetMap(map) => { gameMap = map; sender ! Ack }

    case Trajectory(targetPos, timeToTarget) => actor {
      if (amLeader && !sleeping) {
        flood(BirdApproaching(targetPos, clock.tick()))
        flood(BirdLanded(timeToTarget))
        
        // Collect statuses
        buffer = cache.filter { case (k,v) => neighborsByPort.keys.contains(k) }
        log.debug("Buffer size before status flood: %d" format buffer.size)
        for (n <- neighbors ++ Seq(this)){
          log.debug(n.toString)
          (n !? Status()) match { 
            case WasHit(port, isHit) => buffer.put(port, isHit)
          }
        }
        log.debug("Leader sending db update with buffer size: %d" format buffer.size)

        Thread.sleep(1000)
        // send the _other_ leader's port
        db !? Update(secondaryLeaderPort.get, buffer)
        commit(buffer)
        log.debug("Commit")
        sender ! Ack
      }
    }

    case BirdApproaching(targetPos, incomingClock) => {
      if(!dead){
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
    }

    case BirdLanded(incomingClock) => hitTime = Some(clock.copy())
    case Status()                  => {
      if(!dead) dead = impacted && checkIfHit(moveTime, hitTime)
      sender ! WasHit(port, dead)
    }

    // Getters and Setters
    case GetPort()      => sender ! Port(port)
    case GetPosition()  => sender ! Position(currentPos)
    case SetPosition(x) => { currentPos = x; sender ! Ack }
    case GetStatusMap() => { log.debug("Preparing to send cache..."); sender ! Statuses(cache) }
    case DatabasePush(updates) => {
      log.debug("Preparing to update with Database-pushed changeset.");
      updates.foreach { case (k,v) => cache.update(k,v) }
      sender ! Ack
    }
  }
}


//
// Running it all.
//

class DB(val port: Int) extends AbstractNode with Neighbors with Database

class Pig(val port: Int) extends AbstractNode with Neighbors with RingBasedLeaderElection with LamportClock with PigGameLogic with DatabaseConnection with SecondaryLeader with FaultTolerance

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
      leaders  : Seq[Option[AbstractNode]],
      pigs     : Seq[AbstractNode],
      world    : Seq[Option[Int]],
      exit     : Boolean = true): Map[Int, Boolean] = {

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

    for (leader <- leaders.flatten)
      leader ! Trajectory(targetPos, timeToTarget)

    Thread.sleep(2000)
    log.debug("Done Sleeping... Check final statuses...")

    val s: Map[Int, Boolean] = (leaders.flatten.head !? GetStatusMap()) match {
      case Statuses(map) => map.toMap
      case _ => Map()
    }
    
    log.debug("Done statusing (status size: %d): \n%s" format (s.size, s.mkString("\n")))
    prettyPrint(targetPos, s.toSeq, world)
    s
  }
  
  
  def launch(leaders: Seq[Option[Pig]]): Map[Int, Boolean] = {
    val world = generateMap()
    val target = pickTarget
    launch(target, leaders, pigs, world, exit = false)
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
  var BASE_PORT = 10000
  var DB_PORT = 9999 
  val ELECTION_TIMEOUT = 300 //ms
  val DB_TIMEOUT = 300 //ms
  val CHECK_AWAKE_TIMEOUT = 100 //ms
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
    ).toSeq

    pigs -> ports
  }

  def partitionPigs(pigs: Seq[Pig], ids: Seq[Int]): Seq[Seq[(Pig, Int)]] =
    pigs.zip(ids).grouped(math.ceil(pigs.size / 2.0).toInt).toSeq
 
  def setNeighborsInRingOrder(pigs: Seq[Pig], ids: Seq[Int]): Unit = {
    log.debug("Ring arranger ids: %s" format (ids.mkString(", ")))  
    for ((pig, neighbors) <- (pigs.zip(Stream.continually(ids).flatten.sliding(ids.size).map(_.drop(1).toArray.toSeq).toSeq)))
      pig !? NeighborMessages.SetNeighbors(neighbors)
  }
  
  def startDb(ports:Seq[Int]): DB =
    new DB(Constants.DB_PORT).withEffect { db => 
      db.start()
      db.initMap(ports)
      log.info("Started db on port: %d" format Constants.DB_PORT)
    }

      
  def main(args: Array[String]): Unit = {
    val numPigs = args(0).toInt
    val worldSizeRatio = args(1).toDouble
    
    val (pigs, ports) = startPigs(numPigs)
    
    val part = partitionPigs(pigs, ports).withEffect(x => assert(x.size == 2))
    val (pigs1, ports1) = (part(0).map(_._1), part(0).map(_._2))
    val (pigs2, ports2) = (part(1).map(_._1), part(1).map(_._2))
    
    log.debug("Starting the database..")
    startDb(ports)

    setNeighborsInRingOrder(pigs1, ports1)
    setNeighborsInRingOrder(pigs2, ports2)
    
    log.debug("Sending DebugNeighbors..")
      pigs.map(_ ! NeighborMessages.DebugNeighbors)
 
      log.debug("Initiating an election in set 1..")
    pigs1.head ! RingBasedElectionMessages.Election()
    log.debug("Initiating an election in set 2..")
    pigs2.head ! RingBasedElectionMessages.Election()
    
    Thread.sleep(1500)

    // Find the two leaders
    val leaders = pigs.filter(_.amLeader).map(x => Some(x)).withEffect(x => assert(x.size == 2))
    
    // Introduce the leaders:
    leaders(0).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(1).get.port)
    leaders(1).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(0).get.port)
   
    val ge = new GameEngine(pigs, worldSizeRatio)

    log.info("generating the map..")
    val world = ge.generateMap()
    
    val statuses = for (k <- 1 to 1) yield {
      
      if (k == 3) {
        log.debug("Putting leader %d to sleep." format leaders(0).get.port)
        leaders(0).get !? FaultToleranceMessages.Sleep()
      }
      
      //Check if leader sleeping
      leaders(0).get ! FaultToleranceMessages.CheckIfAwake()
      leaders(1).get ! FaultToleranceMessages.CheckIfAwake()

      Thread.sleep(1000)
      
      //
      // Start the game.
      //
      log.info("launching...")
      val target = ge.pickTarget
      val status = ge.launch(target, leaders.filterNot(x => (x == None || x.get.sleeping)), pigs, world, exit = false)

      Thread.sleep(500)

      // Update Map
      {
        log.debug("Updating map with new positions.")
        // remove all the pigs, keeping the columns
        world.zipWithIndex.map { 
          case (Some(value), i) => if (value != COLUMN) world(i) = None
          case (None, i)        => ()
        }
        // Add back the pigs in their new positions
        pigs.filterNot(_.dead).map(p => world(p.currentPos) = Some(p.port))
      }

      status
    }
    
    // Shutdown all the pigs...
    log.debug("Sending exits...")
    pigs.map(_ !? (50, Exit))
    Constants.BASE_PORT += 100
    
    val (_,exp) = stats(statuses)
    log.info("expected # dead pigs: " + exp)
    System.exit(0)
    
  }

  def stats(maps: Seq[Map[Int,Boolean]]) = {
    val deadPigs = maps.map(_.filter(_._2).size).sum
    val numRounds = maps.length
    val expectation = deadPigs.toDouble / numRounds
    (deadPigs, expectation)
  }


}
