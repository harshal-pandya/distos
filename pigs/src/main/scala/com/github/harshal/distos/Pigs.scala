package com.github.harshal.distos

import scala.actors.Actor
import util.Random
import Constants._
import Messages._
import com.sun.j3d.loaders.lw3d.TargaReader

object Constants {
  val ROUND_TIME = 1000
}

object Messages {
  
  // Game Engine => ()
  case class Start

  // Game Engine => Pig
  case class Map(map: Seq[Option[Entity]])
  case class Trajectory(x: Int)
  case class Status
  case class EndGame(x: Int)
  
  // Pig => Game Engine
  case class Done
  case class WasHit(status: Boolean)
  
  type GameMap = Seq[Option[Entity]]
  
  // Pig => Pig
  case class BirdApproaching(position: Int, hopCount: Int)
  case class TakeShelter(affectedPig: Pig)
  
}


abstract class Entity 

class Pig extends Entity with Actor {
  
  override def toString() = "Pig(%d)" format currentPos
  
  var left:Pig = null
  var right:Pig = null
  var currentPos:Int = -1
  var gameOver = false
  var hit = false
  
  var gameMap: GameMap = null
  
  def available(pos: Int): Boolean = {
    if (pos < 0 || gameMap.size <= pos)
      return false
 
    gameMap(pos) match {
      case None => true
      case _ => false
    }
  }
  
  def move() {
    if (available(currentPos - 1))
      currentPos -= 1
    else if (available(currentPos + 1))
      currentPos += 1
    else
      ()
  }
  
  def act() {
    loop {
      react {
        case Map(map) => { 
          this.gameMap = map
          sender ! Done
        }
        case EndGame(targetPos: Int) => {
          // XXX: fancier later (include columns)
          if (currentPos == targetPos)
            hit = true
        }
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
          if (currentPos == targetPos && !gameOver)
            move()
        }
        case Status => {
          sender ! WasHit(hit)
        }
        case _ => throw new Error("Unknown message")
      }
    }
  }
}

class StoneColumn(pos:Int) extends Entity

class GameEngine(numPigs:Int) {
  
  private val rand = new Random()
  
  private val worldSize = 3 * numPigs
  
  def generateTopology: Seq[Pig] ={
    var prev:Pig = null
    var first:Pig = null
    
    val pigs = (1 to numPigs).map(i => { new Pig })
    
    for ((prev, curr) <- pigs.zip(pigs.drop(1))) {
      prev.left = curr
      curr.right = prev
    }
    
    pigs.head.left  = pigs.last
    pigs.last.right = pigs.head
    
//    val pigs = {
//      for (i <- 1 to numPigs) yield {
//        val pig = new Pig
//        
//        if (i==1) {
//          first = pig
//        }
//        else if(i==numPigs){
//          pig.left = prev
//          prev.right = pig
//          first.left=pig
//        }
//        else{
//          pig.left = prev
//          prev.right = pig
//        }
//        
//        prev = pig
//        pig
//      }
//    }
//    
    pigs
  }

  def generateMap: (Seq[Pig], Array[Option[Entity]]) = {
    
    val worldSize = 3 * numPigs
    val world = Array[Option[Entity]](Seq.fill(worldSize)(None):_*)
    //Generate a random number of columns bounded by the number of pigs
    val numColumns = rand.nextInt(numPigs)
    //Generate a random permutation of the array indices
    val posVector: Seq[Int] = rand.shuffle(0 until worldSize)

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
  
  def launch() {
    
    val (pigs, world) = generateMap
    val targetPos = pickTarget
    val target = world(targetPos)
    val nearest: Pig = pigs.sortBy(_.currentPos).head
    
    for (pig <- pigs)
      pig.start()
      
    // Send out the maps
    for (pig <- pigs)
      pig !? Map(world)
    
    nearest ! Trajectory(targetPos) 
    
    // XXX: add some randomness
    Thread.sleep(ROUND_TIME)

    // End the round
    for (pig <- pigs)
      pig ! EndGame
      
    println(statusAll(pigs).mkString("\n"))

  }
}

object Pigs extends App {
  
  val ge = new GameEngine(4)
  ge.launch()
  
}
