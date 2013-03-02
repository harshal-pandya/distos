package com.github.harshal.distos

import akka.actor._
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout
import util.Random

case object Tick
case object Get

class Counter extends Actor {
  var count = 0

  def receive = {
    case Tick => count += 1
    case Get  => sender ! count
  }
}

object Pigs extends App {


  val system = ActorSystem("Pigs")

  val counter = system.actorOf(Props[com.github.harshal.distos.Counter])

  counter ! Tick
  counter ! Tick
  counter ! Tick

  implicit val timeout = Timeout(5 seconds)

  (counter ? Get) onSuccess {
    case count => println("Count is " + count)
  }

  system.shutdown()
}

abstract class Entity extends Actor

class Pig extends Entity{
  var left:Pig = null
  var right:Pig = null
}

class StoneColumn(pos:Int) extends Entity

class GameEngine(numPigs:Int) extends Actor{
  private val generator = new Random()
  private val worldSize = 3*numPigs
  def generateTopology:Seq[Pig]={
    var prev:Pig = null
    var first:Pig = null
    val pigs = for (i <- 1 to numPigs) yield {
      val pig = new Pig
      if (i==1) {
        first = pig
      }
      else if(i==numPigs){
        pig.left = prev
        prev.right = pig
        first.left=pig
      }
      else{
        pig.left = prev
        prev.right = pig
      }
      prev=pig
      pig
    }
    pigs
  }

  def generateMap:Array[Option[Entity]]={
    val worldSize = 3*numPigs
    val world = Array[Option[Entity]](Seq.fill(worldSize)(None):_*)
    //Generate a random number of columns bounded by the number of pigs
    val numColumns = generator.nextInt(numPigs)
    //Generate a random permutation of the array indices
    val posVector = generator.shuffle(0 until worldSize)

    val pigs = generateTopology.zip(posVector.take(numPigs))

    val columnPos = posVector.takeRight(numColumns)
    for ((pos,pig)<-pigs) world(pos) = Some(pig)
    for (pos<-columnPos) world(pos) = Some(new StoneColumn(pos))
    world
  }
  def pickTarget = generator.nextInt(worldSize-1)
  def launch{
    val world = generateMap
    val target = world(pickTarget)
    val nearest = world.get(world.indexWhere(_.isInstanceOf[Pig]))

  }
}
