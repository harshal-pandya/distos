package com.github.harshal.distos

import org.junit.Test
import org.junit.Before
import org.junit.Assert._

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import com.github.harshal.distos.Constants._
import com.github.harshal.distos.Messages._
import com.github.harshal.distos.Pigs.startPigs

@Test
class PigsTest {
  
  @Before
  def setClassPath() {
    RemoteActor.classLoader = getClass().getClassLoader()
    Constants.BASE_PORT += 100
    println("-----------------------------------")
  }
  
  @Test
  def test0() = {
    val numPigs = 1
    startPigs(numPigs)
    val pig = select(Node("localhost", Constants.BASE_PORT + 1), Symbol((Constants.BASE_PORT+1).toString))
    println(pig !? GetPosition())
    pig ! Exit
  }

  // One pig on a world of size one.
  @Test
  def test1() = {
    
    val numPigs = 1
    startPigs(numPigs)
    val ge = new GameEngine(numPigs, 1)
    val (pigs, world) = ge.generateMap(permutFn = identity)
    val target = 0
    
    val statuses = ge.launch(target, pigs, world)
    
    assert(statuses.size == 1) // we only have one pig
    assert(statuses.head._2)   // assert the pig was hit
  }
  
  @Test
  def test2() = {
    
    val numPigs = 2
    val pigPorts = startPigs(numPigs)
    val ge = new GameEngine(numPigs, 2)
    val pigs = ge.generateTopologyWithoutPorts
    
    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(2)
    
    val world = Seq(
        Some(pigPorts(0)),
        Some(COLUMN),
        Some(pigPorts(1)),
        None)
    
    val target = 0
    
    val statuses = ge.launch(target, pigs, world)
    
    ge.stats(target, statuses, world)
    
    // we only have two pigs
    assert(statuses.size == 2) 
    
    // the first one was trapped by the column
    assert(statuses.head._2)   // assert the first pig was hit
    
    // the second one should have moved
    assert(statuses(1)._1 == 3) // assert the second pig moved from 3 -> 2
    assert(statuses(1)._2 == false) // assert the second pig was not killed
    
  }
  
  @Test
  def test3() = {
    
    val numPigs = 2
    val pigPorts = startPigs(numPigs)
    val ge = new GameEngine(numPigs, 2)
    val pigs = ge.generateTopologyWithoutPorts
    
    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(1)
    
    val world = Seq(
        Some(pigPorts(0)),
        Some(pigPorts(1)),
        None,
        None)
    
    val target = 0
    
    val statuses = ge.launch(target, pigs, world)
    
    ge.stats(target, statuses, world)
    
    // we only have two pigs
    assert(statuses.size == 2) 
    
    // the first one was trapped by the second pig
    assert(statuses.head._2)   // assert the first pig was hit
    
    // the second one should have moved
    assert(statuses(1)._1 == 2) // assert the second pig moved from 1 -> 2
    // ..and not been killed
    assert(statuses(1)._2 == false) // assert the second pig was not killed
    
  }

  @Test
  def test4() = {
    
    val numPigs = 3
    val pigPorts = startPigs(numPigs)
    val ge = new GameEngine(numPigs, 2)
    val pigs = ge.generateTopologyWithoutPorts
    
    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(1)
    pigs(2) !? SetPosition(2)
    
    val world = Seq(
        Some(pigPorts(0)),
        Some(pigPorts(1)),
        Some(pigPorts(2)),
        None,
        None,
        None)
    
    val target = 0
    
    val statuses: Seq[(Int, Boolean)] = ge.launch(target, pigs, world)
    
    ge.stats(target, statuses, world)
    
    // we only have three pigs
    assert(statuses.size == 3) 
    
    // the first one was trapped by the second pig
    assert(statuses.head._1 == 0)   // assert the first pig didn't move
    assert(statuses.head._2)   // assert the first pig was hit
    
    // the second one was trapped by the third pig
    assert(statuses(1)._1 == 1)   // assert the second pig didn't move
    assert(statuses(1)._2)   // assert the second pig was hit
    
    // the third one should have moved
    assert(statuses(2)._1 == 2) // assert the third pig moved from 2 -> 3
    // ..and not been killed
    assert(statuses(2)._2 == false) // assert the third pig was not killed
    
  }

  @Test
  def test5() = {
    
    val numPigs = 1
    val pigPorts = startPigs(numPigs)
    val ge = new GameEngine(numPigs, 3)
    val pigs = ge.generateTopologyWithoutPorts
    
    pigs(0) !? SetPosition(1)
    
    val world = Seq(
        Some(COLUMN),
        Some(pigPorts(0)),
        None)
    
    val target = 0
    
    val statuses = ge.launch(target, pigs, world)
    
    ge.stats(target, statuses, world)
    
    // we only have one pig
    assert(statuses.size == 1) 
    
    // the first one should move 1 -> 2
    assert(statuses.head._1 == 2) //moved
    assert(statuses.head._2 == false)   // wasn't hit
    
  }

}
