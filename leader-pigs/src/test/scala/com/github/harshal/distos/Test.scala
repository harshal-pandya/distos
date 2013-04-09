package com.github.harshal.distos

import org.junit.Test
import org.junit.Before
import org.junit.Assert._

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

import Constants._
import PigsRunner._

@Test
class LeaderElectionTest {
  
  @Before
  def setClassPath() {
    RemoteActor.classLoader = getClass().getClassLoader()
    println("-----------------------------------")
  }
  
  @Test
  def test0() = {
    val numPigs = 1
    val (_, ports) = startPigs(numPigs)
    val pig = select(Node("localhost", ports.head), Symbol((ports.head).toString))
    pig ! Exit
  }

  // One pig on a world of size one.
//  @Test
//  def test1() = {
//    
//    val numPigs = 1
//    startPigs(numPigs)
//    val ge = new GameEngine(numPigs, 1)
//    val (pigs, world) = ge.generateMap(permutFn = identity)
//    val target = 0
//    
//    val statuses = ge.launch(target, pigs, world)
//    
//    assert(statuses.size == 1) // we only have one pig
//    assert(statuses.head._2)   // assert the pig was hit
//  }
  
}
