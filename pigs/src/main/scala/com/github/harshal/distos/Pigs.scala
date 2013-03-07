package com.github.harshal.distos

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.util.Random

import Constants._
import Messages._

object Constants {
  val ROUND_TIME = 1000
  val BASE_PORT  = 10000
}

object Messages {
  
  // Game Engine => ()
  case class Start()

  // Game Engine => Pig
  case class Map(map: Seq[Option[Entity]])
  case class Trajectory(x: Int)
  case class Status()
  case class EndGame()
  
  // Pig => Game Engine
  case class Done()
  case class WasHit(status: Boolean)
  
  type GameMap = Seq[Option[Entity]]
  
  // Pig => Pig
  case class BirdApproaching(position: Int, hopCount: Int)
  case class TakeShelter(position: Int, hopCount: Int)
  
}

abstract class Entity 

case class StoneColumn(pos:Int) extends Entity

class Pig extends Entity with Actor {
  
  override def toString() = "Pig(%d)" format currentPos
  
  var left:Pig = null
  var right:Pig = null
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

  def moveIfRequired(targetPos:Int):Boolean = {
    if (isMoveRequired(targetPos)){
      //if unable to move return true
      !move(currentPos+1)
    }
    else {
      false
    } //if safe return false
  }

  def isMoveRequired(targetPos:Int):Boolean = {
    if(currentPos-1 == targetPos){
      true
    }
    else if(currentPos-2 == targetPos && isColumn(currentPos-1)){
      true
    }
    else false
  }

  def isColumn(pos:Int):Boolean = {
    if (validPos(pos)){
      gameMap(pos).isInstanceOf[Option[StoneColumn]]
    }else{
      false
    }
  }

  def isNotEmpty(pos:Int):Boolean = {
    if (validPos(pos)){
      gameMap(pos) match {
        case None => false
        case _ => true
      }
    }else{
      false
    }
  }

  def checkIfHit(targetPos:Int):Boolean={
    if(targetPos==currentPos) return true
    else if (targetPos==currentPos-1 && isNotEmpty(targetPos)) return true
    else if (targetPos==currentPos-2 && isNotEmpty(targetPos) && isColumn(currentPos-1)) return true
    else return false
  }

  def validPos(pos:Int) = if (pos < 0 || gameMap.size <= pos) false else true
  
  def act() {
    alive(BASE_PORT + currentPos)
    register(Symbol(currentPos.toString), self)
    loop {
      react {
        case Map(map) => { 
          this.gameMap = map
          sender ! Done
        }
        case EndGame => { gameOver = true; sender ! Done }
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
        case Status => {
          sender ! WasHit(hit)
        }
        case Exit => exit()
        case m => throw new Error("Unknown message: " + m)
      }
    }
  }
}

class GameEngine(numPigs: Int, worldSizeRatio: Int = 2) {
  
  private val rand = new Random()
  
  private val worldSize = worldSizeRatio * numPigs
  
  def generateTopology: Seq[Pig] ={
    
    val pigs = (1 to numPigs).map(_ => new Pig)
    
    for ((prev, curr) <- pigs.zip(pigs.drop(1))) {
      prev.right = curr
      curr.left  = prev
    }
    
    pigs.head.left  = pigs.last
    pigs.last.right = pigs.head
    
    pigs
  }

  def generateMap(permutFn: Seq[Int] => Seq[Int] = rand.shuffle(_)): (Seq[Pig], Array[Option[Entity]]) = {
    
    val world = Array.fill[Option[Entity]](worldSize)(None)
    
    //Generate a random number of columns bounded by the number of pigs
    val numColumns = rand.nextInt(numPigs)
    
    //Generate a random permutation of the array indices
    val posVector: Seq[Int] = permutFn(0 until worldSize)

    val pigs: Seq[(Pig, Int)]  = generateTopology.zip(posVector.take(numPigs))

    val columnPos: Seq[Int] = posVector.takeRight(numColumns)
    
    for ((pig, pos) <- pigs) {
      pig.currentPos = pos
      world(pos) = Some(pig)
    }
    
    for (pos <- columnPos) 
      world(pos) = Some(new StoneColumn(pos))
      
    (pigs.map(_._1), world)
  }
  
  def pickTarget = rand.nextInt(worldSize-1)
  
  import Messages._
  
  def statusAll(pigs: Seq[Pig]): Seq[(Pig, Boolean)] =
    (for (p <- pigs) yield
      p -> ((p !? Status) match { case WasHit(hit) => hit })
    )

  def launch(targetPos: Int, pigs: Seq[Pig], world: Seq[Option[Entity]]): Seq[(Pig, Boolean)] = {
    
    val target = world(targetPos)
    val nearest: Pig = pigs.sortBy(_.currentPos).head
    
    for (pig <- pigs)
      pig.start()
      
    for (pig <- pigs)
      pig !? Map(world)
      
    println(world.mkString("\n"))
    
    nearest ! Trajectory(targetPos) 
    
    // random time between 500 and 2000 ms
    val timeToTarget = rand.nextInt(1499)+500
    Thread.sleep(timeToTarget)

    // End the round
    for (pig <- pigs)
      pig !? EndGame
      
    Thread.sleep(1000)
    
    val statuses = statusAll(pigs)
    
    // End the round
    for (pig <- pigs)
      pig ! Exit
      
    statuses

  }
  
  def launch() {
    val (pigs, world) = generateMap()
    val target = pickTarget
    stats(target, launch(target, pigs, world), world)
  }
  
  def stats(target: Int, statuses: Seq[(Pig, Boolean)], world: Seq[Option[Entity]]) {
      
    println("---------------------")
    println(world.mkString("\n"))
    println("---------------------")
    println("target: " + target)
    println("---------------------")
    println(statuses.mkString("\n"))
    
  }

}

object Pigs extends App {
  
  val ge = new GameEngine(4)
  ge.launch()
  
}
