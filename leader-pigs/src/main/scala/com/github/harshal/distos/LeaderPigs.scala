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
import java.util.UUID
import javax.net.ssl.SSLEngineResult.Status

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

  val COLUMN = -2
}

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
  
  def flood(msg: Any) = for (n <- neighbors.values) n ! msg

}

//
// Lamport Clock
//
case class Clock {
  @volatile var _clockValue: Int = 0
  def tick(): Clock = { _clockValue += 1; this }
  def setMax(that: Clock) = _clockValue = scala.math.max(this._clockValue, that._clockValue)
  def copy() = { val c = new Clock; c._clockValue = _clockValue; c }
}

trait LamportClock {
  val clock: Clock = new Clock
} 

//
// Leader Election
//

// Messages required by the `RingBasedLeaderElection` trait.
object RingBasedElectionMessages {
  
  // An Election message has an id and the process id's
  // of all the nodes it has been passed through.
  // It is also used to initiate an Election.
  case class Election(procIds: Seq[Int] = Seq.empty) {
    val id  = UUID.randomUUID().toString.take(8)
    def max = procIds.max
  }
  // A leader message informs the elected leader of the result.
  case class Leader(port: Int)
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
    case Leader(p) => {
      log.debug("%d setting leader to %d" format(port,p))
      leader = Some(if (p == port) this else neighbors(p))
      sender ! Ack
    }
    case e: Election => {
      sender ! Ack
      // If the message contains our port then we've gone around the circle.
      if (e.procIds.contains(port)) {
        log.info("Election %s finished: %d is the leader." format(e.id, e.max))
        // Send out the new leader to everyone.
        for ((p,n) <- neighbors) 
          n !? (200, Leader(e.max)) match { 
            case None => log.error("%d did not respond to Leader message" format p)
            case _    => ()
          }
        // Set the new leader for ourself too
        this ! Leader(e.max)
      } else {
        // Otherwise, we should pass it along, skipping
        // neighbors that don't responsd within the timeout.
        val msg = e.copy(procIds = e.procIds ++ Seq(port))
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

// A stone column is simply a position on the map.
case class StoneColumn(position: Int){
  val objectId = -2
}


// TODO
object GameMessages {
  
  // Game Engine => ()
  case class Start()

  // Game Engine => Pig
  case class Map(map: Seq[Option[Int]])
  case class Trajectory(x: Int,ttt:Int)
  case class Status()
  case class StatusResponse(impacted:Boolean,moveTime:Option[Clock],hitTime:Option[Clock])
  case class BirdLanded(clock:Clock)
  case class GetPosition()
  case class GetPort()
  case class Port(x: Int)
  case class Position(x: Int)
  case class SetPosition(x: Int)
  case class SetLeft(x: Int)
  case class SetRight(x: Int)
  
  // Pig => Game Engine
  case class Done()
  case class WasHit(status: Boolean)
  
  type GameMap = Seq[Option[Int]]
  
  // Pig => Pig
  case class BirdApproaching(position: Int, clock: Clock)
  
}


trait PigGameLogic extends AbstractPig {
  this: AbstractPig with Neighbors with RingBasedLeaderElection with LamportClock =>

  //import GameMessages._

  override def actions = super.actions ++ Seq(action)
    
  // There are a bunch of things available to us:
  // this.leader: Option[AbstractActor]  -- the current leader
  // this.port  : Int -- our port
  // this.neighbors: Map[Int, AbstractActor] -- a map from ports to other pigs
  
  var currentPos:Int = -1
  var moveTime: Option[Clock] = None
  var hitTime: Option[Clock] = None
  var impacted: Boolean = false
  var gameMap: GameMessages.GameMap = null


  /**
   * check if this position is empty and available for moving into
   * @param pos
   * @return
   */
  def available(pos: Int): Boolean = {
    if (validPos(pos)){
      gameMap(pos) match {
        case None => true
        case _ => false
      }
    }
    else false
  }


  /**
   * Find an empty spot next to yourself and move if possible
   * @return true if moved successfuly else false
   */
  def move():Boolean =  {
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
   * move to this position
   * @param pos
   * @return
   */
  def move(pos:Int):Boolean={
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
      !move(currentPos+1) //if unable to move return true
    else
      false //if safe return false
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
  def isColumn(pos: Int)  = if (validPos(pos)) gameMap(pos) == Some(COLUMN) else false

  /**
   * Is this postion not empty?
   * @param pos
   * @return
   */
  def isNotEmpty(pos:Int) = if (validPos(pos)) gameMap(pos) != None else false

  /**
   * To check if next position is beyond the map boundary
   * @param pos
   * @return
   */
  def validPos(pos:Int) = !(pos < 0 || gameMap.size <= pos)

  /**
   * compare the Lamport's clock for the move time and the hit time and
   * decide whether you are going to be hit
   * @param moveTime
   * @param hitTime
   * @return
   */
  def checkIfHit(moveTime:Option[Clock],hitTime:Option[Clock]):Boolean = {
    (moveTime,hitTime) match {
      case (Some(mtime),Some(htime)) => mtime._clockValue> htime._clockValue
      case (None,Some(htime)) => true
      case _ => println("Hit time not set!!!!"); false
    }
  }

  private val action: Any ~> Unit = {

    case GameMessages.Map(map) => {
      this.gameMap = map
      sender ! Ack
    }

    case GameMessages.Trajectory(targetPos,timeToTarget) => {
      if (amLeader){
        flood(GameMessages.BirdApproaching(targetPos, clock.tick()))
        Thread.sleep(timeToTarget)
        flood(GameMessages.Status())
      }
    }

    case GameMessages.BirdApproaching(targetPos, incomingClock) => {
      
      clock.setMax(incomingClock)
      Util.simulateNetworkDelay(clock)
      
      // deal with ourselves
      if (targetPos == currentPos - 1 && isColumn(currentPos-1)) {
        // TODO
        impacted=true
        val success = move()
        clock.tick()
        if (success) moveTime = Some(clock.copy())
      }
      else if (currentPos == targetPos){
        impacted=true
        val success = move()
        clock.tick()
        if (success) moveTime = Some(clock.copy())
      }
    }

    // TODO: 
//    case TakeShelter(targetPos,hopCount) => {
//      if (hopCount > 0){
//        left  ! TakeShelter(targetPos, hopCount - 1)
//        right ! TakeShelter(targetPos, hopCount - 1)
//      }
//      if (targetPos!=currentPos && !gameOver){
//        hit = moveIfRequired(targetPos)
//      }
//      else if (targetPos!=currentPos && isMoveRequired(targetPos)) //game over but not yet updated state
//        hit = true
//    }
    case GameMessages.BirdLanded(incomingClock) =>{
      clock.setMax(incomingClock)
      Util.simulateNetworkDelay(clock)
      clock.tick()
      hitTime = Some(clock.copy())
    }
    case GameMessages.Status() => {
      sender ! GameMessages.WasHit(checkIfHit(moveTime,hitTime))
    }
    case GameMessages.WasHit(isHit) => {

      //TODO
    }
    case GameMessages.GetPort() => { sender ! GameMessages.Port(port) }
    case GameMessages.GetPosition() => { sender ! GameMessages.Position(currentPos) }
    case GameMessages.SetPosition(x) => {
      currentPos = x
      sender ! GameMessages.Done
    }
//    case SetLeft(port) => {
//      left = select(Node("localhost", port), Symbol(port.toString))
//      sender ! Done()
//    }
//    case SetRight(port) => {
//      right = select(Node("localhost", port), Symbol(port.toString))
//      sender ! Done()
//    }
    case Exit => {  sender ! GameMessages.Done(); exit() }
    case m => throw new Error("Unknown message: " + m)
  }
}


//
// Running it all.
//

class Pig(val port: Int) extends AbstractPig with Neighbors with RingBasedLeaderElection with LamportClock

class GameEngine(pigs:Seq[Pig] , worldSizeRatio: Int) {
  import GameMessages._

  private val rand = new Random()

  private val numPigs = pigs.size

  private val worldSize = worldSizeRatio * numPigs

//  def generateTopologyWithoutPorts: Seq[AbstractActor] = generateTopology.map(_._2)
//  def generateTopology: Seq[(Int, AbstractActor)] = {
//
//    println("Gathering Pigs...")
//
//    val pigs: Seq[(Int, AbstractActor)] = (1 to numPigs).map(_ + BASE_PORT).map(port => {
//      println("Looking up on port: " + port)
//      val p: AbstractActor = select(Node("localhost", port), Symbol(port.toString))
//      println("Checking alive: " + (p !? GetPosition()))
//      port -> p
//    })
//
//    println("done gathering.")
//
//    println("Can we communicate with the first pig?: " + (pigs.head._2 !? GetPosition()))
//
//    for ((prev, curr) <- pigs.zip(pigs.drop(1))) {
//      prev._2 !? SetRight(curr._1)
//      curr._2 !? SetLeft (prev._1)
//    }
//    println("done setting mid positions")
//
//    pigs.head._2 ! SetLeft (pigs.last._1)
//    pigs.last._2 ! SetRight(pigs.head._1)
//
//    println("done setting end positions")
//
//    pigs
//  }

  def generateMap(permutFn: Seq[Int] => Seq[Int] = rand.shuffle(_)): Array[Option[Int]] = {

    val world = Array.fill[Option[Int]](worldSize)(None)

    //Generate a random number of columns bounded by the number of pigs
    val numColumns = rand.nextInt(numPigs)

    //Generate a random permutation of the array indices
    val posVector: Seq[Int] = permutFn(0 until worldSize)

//    val pigs: Seq[((Int, AbstractActor), Int)]  = generateTopology.zip(posVector.take(numPigs))

    val columnPos: Seq[Int] = posVector.takeRight(numColumns)

    for ((pig,pos) <- pigs.zip(posVector.take(numPigs))) {
//TODO      pig !? SetPosition(pos)
      world(pos) = Some(pig.port)
    }

    val stoneColumns = for (pos <- columnPos) yield {
      val column = new StoneColumn(pos)
      world(pos) = Some(column.objectId)
      column
    }

    world
  }

  def pickTarget = rand.nextInt(worldSize-1)

  def launch(
              targetPos: Int,
              leader:AbstractPig,
              pigs: Seq[AbstractPig],
              world: Seq[Option[Int]],
              exit: Boolean = true) {

    for (pig <- pigs)
      pig !? Map(world)

    println("""
              |  -----------------------
              |  |  Initial locations  |
              |  -----------------------
              |""".stripMargin)
    prettyPrintMap(world)


    // random time between 100 and 1000 ms
    val timeToTarget = rand.nextInt(450) + 550
    println("Time to target: " + timeToTarget)

    leader ! Trajectory(targetPos,timeToTarget)

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
    launch(target,leader,pigs,world,exit=false)
//    prettyPrint(target, launch(target, pigs, world, exit = false), world)
  }

//  def stats(target: Int, statuses: Seq[(Int, Boolean)], world: Seq[Option[Int]]) {
//
//    println("---------------------")
//    println(world.mkString("\n"))
//    println("---------------------")
//    println("target: " + target)
//    println("---------------------")
//    println(statuses.mkString("\n"))
//
//  }

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
    val worldSizeRatio = args(1).toInt
    val (pigs, ports) = startPigs(numPigs)
    setNeighborsInRingOrder(pigs, ports)
    log.debug("Sending DebugNeighbors..")
    pigs.map(_ ! NeighborMessages.DebugNeighbors)
    
    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()
    
    Thread.sleep(1000)
    
    log.debug("Killing the leader..")
    pigs.last !? Exit
    
    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()
    
    
    // TODO: Start the game here.
    val leader = pigs.last //TODO confirm this
    val ge = new GameEngine(pigs,worldSizeRatio)
    ge.generateMap()
    ge.launch(pigs.last)


    Thread.sleep(3000)
    log.debug("Sending exits..")
    pigs.map(_ !? (500, Exit))
 
    System.exit(0)
  }

}
