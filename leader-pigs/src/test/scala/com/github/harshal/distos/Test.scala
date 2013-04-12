package com.github.harshal.distos

import org.junit.Test
import org.junit.Before
import scala.actors._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import GameMessages._
import PigsRunner._
import com.codahale.logula.Logging
import Util.COLUMN

@Test
class LeaderElectionTest extends Logging{
  
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

  @Test
  def leaderElection() = {
    val numPigs = 1
    val (pigs, ports) = startPigs(numPigs)

    var max = pigs.map(_.id).max

    setNeighborsInRingOrder(pigs, ports)
    log.debug("Sending DebugNeighbors..")
    pigs.map(_ ! NeighborMessages.DebugNeighbors)

    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()

    Thread.sleep(1000)

    var leader = pigs.find(_.amLeader) match {
      case Some(pig) => pig
      case None      => throw new Exception("Runner could not find the leader.")
    }


    assert(leader.id.equals(max))

    log.debug("Killing the leader..")
    leader !? Exit

    val candidate = pigs.find(_.id!=max).get

    log.debug("Initiating an election..")
    pigs.find(_.id!=max).get ! RingBasedElectionMessages.Election()

    Thread.sleep(1000)

    val newPigs = pigs.filterNot(_.id==max)

    max = newPigs.map(_.id).max


    leader = newPigs.find(_.amLeader) match {
      case Some(pig) => pig
      case None      => throw new Exception("Runner could not find the leader.")
    }


    assert(leader.id.equals(max))

    Thread.sleep(3000)
    log.debug("Sending exits..")
    pigs.map(_ !? (500, Exit))

    System.exit(0)
  }

  // One pig on a world of size one.
  @Test
  def test1() = {

    val numPigs = 1
    val worldSizeRatio = 3
    val (pigs, ports) = startPigs(numPigs)
    val leader = runLeaderElection(pigs,ports)

    //
    // Start the game.
    //
    val ge = new GameEngine(pigs, worldSizeRatio)

    log.info("generating the map..")
    val world = ge.generateMap(permutFn = identity)

    val target = 0

    log.info("launching...")
    val statusMap = ge.launch(target, leader, pigs, world)

    Thread.sleep(3000)
    log.debug("Sending exits..")
    pigs.map(_ !? (50, Exit))

    assert(statusMap.size == 1)
    assert(statusMap.head._2)

    System.exit(0)
//
//    startPigs(numPigs)
//    val ge = new GameEngine(numPigs, 1)
//    val (pigs, world) = ge.generateMap(permutFn = identity)
//    val target = 0
//
//    val statuses = ge.launch(target, pigs, world)
//
//    assert(statuses.size == 1) // we only have one pig
//    assert(statuses.head._2)   // assert the pig was hit
  }

  @Test
  def test2() = {
    val numPigs = 2
    val worldSizeRatio = 2
    val (pigs, ports) = startPigs(numPigs)
    val leader = runLeaderElection(pigs,ports)
    val ge = new GameEngine(pigs, worldSizeRatio)

    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(2)

    val world = Seq(
      Some(pigs(0).port),
      Some(COLUMN),
      Some(pigs(1).port),
      None)

    val target = 0

    log.info("launching...")
    val statusMap = ge.launch(target,leader,pigs,world)

//    ge.stats(target, statuses, world)

    // we only have two pigs
    assert(statusMap.size == 2)

    // the first one was trapped by the column
    assert(statusMap(pigs(0).id))   // assert the first pig was hit

    // the second one should have moved
    val pos = pigs(1)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 3, "incorrect: " + statusMap(pigs(1).id)) // assert the second pig moved from 3 -> 2
    assert(statusMap(pigs(1).id) == false) // assert the second pig was not killed

  }

  @Test
  def test3() = {

    val numPigs = 2
    val (pigs, ports) = startPigs(numPigs)
    val leader = runLeaderElection(pigs,ports)
    val ge = new GameEngine(pigs, 2)


    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(1)

    val world = Seq(
      Some(pigs(0).port),
      Some(pigs(1).port),
      None,
      None)

    val target = 0

    val statuses = ge.launch(target,leader, pigs, world)

//    ge.stats(target, statuses, world)

    // we only have two pigs
    assert(statuses.size == 2)

    // the first one was trapped by the second pig
    assert(statuses(pigs(0).id))   // assert the first pig was hit

    // the second one should have moved
    val pos = pigs(1)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 2) // assert the second pig moved from 1 -> 2
    // ..and not been killed
    assert(statuses(pigs(1).id) == false) // assert the second pig was not killed

  }

  @Test
  def test4() = {

    val numPigs = 3
    val (pigs, ports) = startPigs(numPigs)
    val leader = runLeaderElection(pigs,ports)
    val ge = new GameEngine(pigs, 2)

    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(1)
    pigs(2) !? SetPosition(2)

    val world = Seq(
      Some(pigs(0).port),
      Some(pigs(1).port),
      Some(pigs(2).port),
      None,
      None,
      None)

    val target = 0

    val statuses = ge.launch(target,leader, pigs, world)

//    ge.stats(target, statuses, world)

    // we only have three pigs
    assert(statuses.size == 3)

    // the first one was trapped by the second pig
    var pos = pigs(0)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 0)   // assert the first pig didn't move
    assert(statuses(pigs(0).id))   // assert the first pig was hit

    // the second one was trapped by the third pig
    pos = pigs(1)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 1)   // assert the second pig didn't move
    assert(statuses(pigs(1).id))   // assert the second pig was hit

    // the third one should not have moved
    pos = pigs(2)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 2) // assert the third pig did not move
    // ..and not been killed
    assert(statuses(pigs(2).id) == false) // assert the third pig was not killed

  }
//
  @Test
  def test5() = {

  val numPigs = 1
  val (pigs, ports) = startPigs(numPigs)
  val leader = runLeaderElection(pigs,ports)

  pigs(0) !? SetPosition(1)

    val world = Seq(
      Some(COLUMN),
      Some(pigs(0).port),
      None)


  val ge = new GameEngine(pigs,3)
    val target = 0

    val statuses = ge.launch(target,leader, pigs, world)

//    ge.stats(target, statuses, world)

    // we only have one pig
    assert(statuses.size == 1)

    // the first one should move 1 -> 2
    val pos = pigs(0)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 2, "incorrect: " + statuses.head._1) //moved
    assert(statuses(pigs(0).id) == false)   // wasn't hit

  }

  def runLeaderElection(pigs:Seq[Pig],ports:Seq[Int])={
    setNeighborsInRingOrder(pigs, ports)
    log.debug("Sending DebugNeighbors..")
    pigs.map(_ ! NeighborMessages.DebugNeighbors)

    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()

    Thread.sleep(1000)


    // Find the leader
    val leader = pigs.find(_.amLeader) match {
      case Some(pig) => pig
      case None      => throw new Exception("Runner could not find the leader.")
    }
    leader
  }
}
