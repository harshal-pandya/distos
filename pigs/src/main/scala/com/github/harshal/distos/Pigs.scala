package com.github.harshal.distos

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.util.Random
import Constants._
import Messages._
import scala.actors.Futures._

object Constants {
  val ROUND_TIME = 1000
  var BASE_PORT  = 10000
  
  val COLUMN = -2
}

object Messages {
  
  // Game Engine => ()
  case class Start()

  // Game Engine => Pig
  case class Map(map: Seq[Option[Int]])
  case class Trajectory(x: Int)
  case class Status()
  case class EndGame()
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
  case class BirdApproaching(position: Int, hopCount: Int)
  case class TakeShelter(position: Int, hopCount: Int)
  
}

abstract class PigRef extends AbstractActor

class StoneColumn(pos:Int) extends AbstractActor with Actor { def act() {} }

class Pig(val port: Int) extends PigRef with Actor {
  
  override def toString() = "Pig(%d)" format currentPos
  
  var left:  AbstractActor = null
  var right: AbstractActor = null
  var currentPos:Int = -1
  var gameOver = false
  var hit = false
  
  var gameMap: GameMap = null
  
  def available(pos: Int): Boolean = {
    if (validPos(pos)){
      gameMap(pos) match {
        case None => true
        case _ => false
      }
    }
    else false
  }
  
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

  def move(pos:Int):Boolean={
    if (available(pos)) {
      currentPos = pos
      true
    }
    else false
  }

  def moveIfRequired(targetPos: Int) = {
    if (isMoveRequired(targetPos))
      !move(currentPos+1) //if unable to move return true
    else
      false //if safe return false
  }

  def isMoveRequired(targetPos: Int) =
    (currentPos-1 == targetPos) || 
    (currentPos-2 == targetPos && isColumn(currentPos-1))

  def isColumn(pos: Int)  = if (validPos(pos)) gameMap(pos) == Some(COLUMN) else false
  def isNotEmpty(pos:Int) = if (validPos(pos)) gameMap(pos) != None else false

  def checkIfHit(targetPos: Int) =
    (targetPos == currentPos) || 
    (targetPos == currentPos - 1 && isNotEmpty(targetPos)) ||
    (targetPos == currentPos - 2 && isNotEmpty(targetPos) && isColumn(currentPos - 1))

  def validPos(pos:Int) = !(pos < 0 || gameMap.size <= pos)
  
  def act() {
    alive(port)
    register(Symbol(port.toString), self)
    loop {
      react {
        case Map(map) => { 
          this.gameMap = map
          sender ! Done
        }
        case EndGame() => { gameOver = true; sender ! Done }
        case Trajectory(targetPos) => {
          // send out the message
          left  ! BirdApproaching(targetPos, gameMap.size)
          right ! BirdApproaching(targetPos, gameMap.size)
        }
        case BirdApproaching(targetPos, hopCount) => { 
          // send out the message
          if (hopCount > 0) {
            left  ! BirdApproaching(targetPos, hopCount - 1)
            right ! BirdApproaching(targetPos, hopCount - 1)
          }
          
          // deal with ourselves
          if (targetPos==currentPos-1 && isColumn(currentPos-1) && !gameOver){
            val success = move()
            if (!success) hit = true
          }
          else if (currentPos == targetPos && !gameOver){
            val success = move()
            if (!success){
              left ! TakeShelter(targetPos,gameMap.size)
              right ! TakeShelter(targetPos,gameMap.size)
              hit = true
            }
          }
          else if (currentPos == targetPos && gameOver) {
            hit = true
          }
        }
        case TakeShelter(targetPos,hopCount) => {
          if (hopCount > 0){
            left  ! TakeShelter(targetPos, hopCount - 1)
            right ! TakeShelter(targetPos, hopCount - 1)
          }
          if (targetPos!=currentPos && !gameOver){
            hit = moveIfRequired(targetPos)
          }
          else if (targetPos!=currentPos && isMoveRequired(targetPos)) //game over but not yet updated state
            hit = true
        }
        case Status() => {
          sender ! WasHit(hit)
        }
        case GetPort() => { sender ! Port(port) }
        case GetPosition() => { sender ! Position(currentPos) }
        case SetPosition(x) => {
          currentPos = x
          hit = false
          sender ! Done
        }
        case SetLeft(port) => {
          left = select(Node("localhost", port), Symbol(port.toString))
          sender ! Done()
        }
        case SetRight(port) => {
          right = select(Node("localhost", port), Symbol(port.toString))
          sender ! Done()
        }
        case Exit => {  sender ! Done(); exit() }
        case m => throw new Error("Unknown message: " + m)
      }
    }
  }
}

class GameEngine(numPigs: Int, worldSizeRatio: Int) {
  
  private val rand = new Random()
  
  private val worldSize = worldSizeRatio * numPigs
  
  def generateTopologyWithoutPorts: Seq[AbstractActor] = generateTopology.map(_._2)
  def generateTopology: Seq[(Int, AbstractActor)] = {
    
    println("Gathering Pigs...")
    
    val pigs: Seq[(Int, AbstractActor)] = (1 to numPigs).map(_ + BASE_PORT).map(port => {
      println("Looking up on port: " + port)
      val p: AbstractActor = select(Node("localhost", port), Symbol(port.toString))
      println("Checking alive: " + (p !? GetPosition()))
      port -> p
    })
    
    println("done gathering.")
    
    println("Can we communicate with the first pig?: " + (pigs.head._2 !? GetPosition()))
    
    for ((prev, curr) <- pigs.zip(pigs.drop(1))) {
      prev._2 !? SetRight(curr._1)
      curr._2 !? SetLeft (prev._1)
    }
    println("done setting mid positions")
    
    pigs.head._2 ! SetLeft (pigs.last._1)
    pigs.last._2 ! SetRight(pigs.head._1)
    
    println("done setting end positions")
    
    pigs
  }

  def generateMap(permutFn: Seq[Int] => Seq[Int] = rand.shuffle(_)): (Seq[AbstractActor], Array[Option[Int]]) = {
    
    val world = Array.fill[Option[Int]](worldSize)(None)
    
    //Generate a random number of columns bounded by the number of pigs
    val numColumns = rand.nextInt(numPigs)
    
    //Generate a random permutation of the array indices
    val posVector: Seq[Int] = permutFn(0 until worldSize)

    val pigs: Seq[((Int, AbstractActor), Int)]  = generateTopology.zip(posVector.take(numPigs))
    
    val columnPos: Seq[Int] = posVector.takeRight(numColumns)
    
    for (((port, pig), pos) <- pigs) {
      pig !? SetPosition(pos)
      world(pos) = Some(port)
    }

    for (pos <- columnPos) 
      world(pos) = Some(COLUMN)
      
    (pigs.map(_._1._2), world)
  }
  
  def pickTarget = rand.nextInt(worldSize-1)
  
  import Messages._
  
  def statusAll(pigs: Seq[AbstractActor]): Seq[(Int, Boolean)] =
    (for (p <- pigs) yield (
      ((p !? GetPosition()) match { case Position(x) => x; case _ => -1 }),
      ((p !? Status()) match { case WasHit(hit) => hit })
    ))

  def launch(
      targetPos: Int,
      pigs: Seq[AbstractActor],
      world: Seq[Option[Int]],
      exit: Boolean = true): Seq[(Int, Boolean)] = {
    
    val target = world(targetPos)
    val nearest = (pigs
       .sortBy(p => (p !? GetPosition()) match { 
         case Position(x) => x;
         case _ => -1 }
       ).head)
      
    for (pig <- pigs)
      pig !? Map(world)
      
    println("""
        |  -----------------------
        |  |  Initial locations  |
        |  -----------------------
        |""".stripMargin)
    prettyPrintMap(world)

    nearest ! Trajectory(targetPos) 
    
    // random time between 100 and 1000 ms
    val timeToTarget = rand.nextInt(450) + 550
    println("Time to target: " + timeToTarget)
    Thread.sleep(timeToTarget)

    // End the round
    for (pig <- pigs)
      pig !? EndGame()
      
    Thread.sleep(1000)
    
    val statuses = statusAll(pigs)
    
    if (exit)
      for (pig <- pigs)
        pig !? Exit
 
    statuses

  }
  
  def launch() {
    val (pigs, world) = generateMap()
    val target = pickTarget
    prettyPrint(target, launch(target, pigs, world, exit = false), world)
  }
  
  def stats(target: Int, statuses: Seq[(Int, Boolean)], world: Seq[Option[Int]]) {
      
    println("---------------------")
    println(world.mkString("\n"))
    println("---------------------")
    println("target: " + target)
    println("---------------------")
    println(statuses.mkString("\n"))
    
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

object Game extends App {
  
  RemoteActor.classLoader = getClass().getClassLoader()
  
  val ge = new GameEngine(args(0).toInt, args(1).toInt)
  ge.launch()
  
  System.exit(0)
}

object Pigs {

  RemoteActor.classLoader = getClass().getClassLoader()
  
  def startPigs(numPigs: Int): Seq[Int] =
    (for (port <- (1 to numPigs).map(_ + BASE_PORT)) yield {
      println("Starting pig on port: " + port)
      new Pig(port).start()
      port
    }).toSeq

  def main(args: Array[String]): Unit = {
    val numPigs = args(0).toInt
    startPigs(numPigs)
  }
  
}

