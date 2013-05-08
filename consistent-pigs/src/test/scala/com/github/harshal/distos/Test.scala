package com.github.harshal.distos

import org.junit.Test
import org.junit.Before
import scala.actors._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import GameMessages._
import PigsRunner._
import com.codahale.logula.Logging
import Util._
import org.apache.log4j.Level

@Test
class LeaderElectionTest extends Logging {

  @Before
  def setClassPath() {
    RemoteActor.classLoader = getClass().getClassLoader()
    Constants.BASE_PORT += 100
    println("-----------------------------------")
  }

  @Test
  def test0(): Unit = {
    val numPigs = 1
    val (_, ports) = startPigs(numPigs)
    val pig = select(Node("localhost", ports.head), Symbol((ports.head).toString))
    pig !? Exit
  }

  def findLeader(pigs: Seq[Pig]): Pig = pigs.find(_.amLeader) match {
    case Some(pig) => pig
    case None      => throw new Exception("Runner could not find the leader.")
  }

  //  def findLeaderId(pigs: Seq[Pig]): String = (pigs.head !? RingBasedElectionMessages.WhoIsLeader()) match {
  //    case RingBasedElectionMessages.LeaderId(Some(id)) => id
  //    case RingBasedElectionMessages.LeaderId(None)     => throw new Exception("Runner could not find the leader.")
  //  }

  @Test
  def leaderElection(): Unit = {
    val numPigs = 5
    val (pigs, ports) = startPigs(numPigs)
    setNeighborsInRingOrder(pigs, ports)

    val max = pigs.maxBy(_.id).id
    log.debug("max id: %s" format max)

    log.debug("Initiating an election..")
    pigs.head ! RingBasedElectionMessages.Election()
    Thread.sleep(500)

    // Ensure we've found the correct leader
    //val leaderId = findLeaderId(pigs)
    val leader = findLeader(pigs)
    log.debug("Asserting leader is correct..")
    //assert(leaderId.equals(max))
    assert(leader.id.equals(max))

    log.debug("Killing the leader..")
    pigs.find(_.id==max).get !? Exit

    log.debug("Initiating an election..")
    pigs.find(_.id != max).get ! RingBasedElectionMessages.Election()
    Thread.sleep(500)

    val newPigs = pigs.filterNot(_.id == max)
    val newMax  = newPigs.map(_.id).max
    //    val newLeaderId = findLeaderId(newPigs)
    val newLeader = findLeader(newPigs)

    //    assert(newLeaderId.equals(newMax))
    assert(newLeader.id.equals(newMax))

    log.debug("Sending exits..")
    newPigs.map(_ !? (500, Exit))
  }

  @Test
  def testCacheConsistency(): Unit = {
    Constants.DEBUG_MODE = true
    enableLogging(false)
    val numPigs = 6
    val (pigs, ports) = startPigs(numPigs)

    val part = partitionPigs(pigs, ports)
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
    val leaders = pigs.filter(_.amLeader).map(x => Some(x))

    // Introduce the leaders:
    leaders(0).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(1).get.port)
    leaders(1).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(0).get.port)

    val ge = new GameEngine(pigs, 2)

    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(1)
    pigs(2) !? SetPosition(2)
    pigs(3) !? SetPosition(6)
    pigs(4) !? SetPosition(7)
    pigs(5) !? SetPosition(9)

    println(pigs(0).port+" and "+pigs(1).port+" should die")

    val world = Seq(
      Some(pigs(0).port),
      Some(pigs(1).port),
      Some(pigs(2).port),
      None,
      None,
      None,
      Some(pigs(3).port),
      Some(pigs(4).port),
      None,
      Some(pigs(5).port),
      None,
      None)

    val target = 0

    ge.launch(target, leaders.filterNot(x => (x == None || x.get.sleeping)), pigs, world, exit = false)

    println("Post-update : "+leaders(0).get.port+" : "+leaders(0).get.cache)
    println("Post-update : "+leaders(1).get.port+" : "+leaders(1).get.cache)

    // Shutdown all the pigs...
    log.debug("Sending exits...")
    pigs.map(_ !? (50, Exit))

  }

  @Test
  def testFaultTolerance(): Unit = {
    enableLogging(false)
    val numPigs = 6
    val (pigs, ports) = startPigs(numPigs)

    val part = partitionPigs(pigs, ports)
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
    val leaders = pigs.filter(_.amLeader).map(x => Some(x))

    println("Leaders are "+leaders(0).get.port+" and "+leaders(1).get.port)

    // Introduce the leaders:
    leaders(0).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(1).get.port)
    leaders(1).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(0).get.port)

    for (k <- 1 to 2) {
      if (k == 2) {
        println("Putting leader %d to sleep." format leaders(0).get.port)
        leaders(0).get !? FaultToleranceMessages.Sleep()
      }

      //Check if leader sleeping
      leaders(0).get ! FaultToleranceMessages.CheckIfAwake()
      leaders(1).get ! FaultToleranceMessages.CheckIfAwake()

      Thread.sleep(1000)

      if (k==1){
        assert(leaders(0).get.neighbors.size==2)
        assert(leaders(1).get.neighbors.size==2)
      }
      else{
        assert(leaders(1).get.neighbors.size==5)
      }
    }

    // Shutdown all the pigs...
    log.debug("Sending exits...")
    pigs.map(_ !? (50, Exit))
  }

  /**
   * This tests if pigs dead once stay dead in the next rounds
   */
  @Test
  def deadPigTest() : Unit = {

    Constants.DEBUG_MODE=true
    enableLogging(true)
    val numPigs = 4
    val worldSizeRatio = 1
    val (pigs, ports) = startPigs(numPigs)

    val part = partitionPigs(pigs, ports)
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
    val leaders = pigs.filter(_.amLeader).map(x => Some(x))

    println("Leaders are "+leaders(0).get.port+" and "+leaders(1).get.port)

    // Introduce the leaders:
    leaders(0).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(1).get.port)
    leaders(1).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(0).get.port)

    leaders(0).get.cache
    leaders(1).get.cache

    val ge = new GameEngine(pigs, worldSizeRatio)

    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(1)
    pigs(2) !? SetPosition(2)
    pigs(3) !? SetPosition(3)

    println(pigs(0).port)
    println(pigs(1).port)

    val world = Array[Option[Int]](
    Some(pigs(0).port),
    Some(pigs(1).port),
    Some(pigs(2).port),
    Some(pigs(3).port))

    for (i <- Seq(0,2)){
      val target = i

      log.info("launching...")
      val statusMap = ge.launch(target,leaders.filterNot(x => (x == None || x.get.sleeping)),pigs,world)

      if (target==0){
        assert(statusMap(pigs(0).port))
        assert(statusMap(pigs(1).port))
        // remove all the pigs, keeping the columns
        world.zipWithIndex.map {
          case (Some(value), i) => if (value != COLUMN) world(i) = None
          case (None, i)        =>
        }
        // Add back the pigs in their new positions
        pigs.filterNot(_.dead).map(p => world(p.currentPos) = Some(p.port))
      }
      else{
        assert(statusMap(pigs(0).port))
        assert(statusMap(pigs(1).port))
      }

    }

  }

  @Test
  def test2(): Unit = {
    Constants.DEBUG_MODE=true
    enableLogging(true)
    val numPigs = 2
    val worldSizeRatio = 2
    val (pigs, ports) = startPigs(numPigs)

    val part = partitionPigs(pigs, ports)
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
    val leaders = pigs.filter(_.amLeader).map(x => Some(x))

    println("Leaders are "+leaders(0).get.port+" and "+leaders(1).get.port)

    // Introduce the leaders:
    leaders(0).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(1).get.port)
    leaders(1).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(0).get.port)

    leaders(0).get.cache
    leaders(1).get.cache

    val ge = new GameEngine(pigs, worldSizeRatio)

    pigs(0) !? SetPosition(0)
    pigs(1) !? SetPosition(2)

    println(pigs(0).port)
    println(pigs(1).port)

    val world = Seq(
      Some(pigs(0).port),
      Some(COLUMN),
      Some(pigs(1).port),
      None)

    val target = 0

    log.info("launching...")
    val statusMap = ge.launch(target,leaders.filterNot(x => (x == None || x.get.sleeping)),pigs,world)

    println(leaders(0).get.port+" : "+leaders(0).get.cache)
    println(leaders(1).get.port+" : "+leaders(1).get.cache)

    //    ge.stats(target, statuses, world)

    // we only have two pigs
    assert(statusMap.size == 2)

    // the first one was trapped by the column
    assert(statusMap(pigs(0).port))   // assert the first pig was hit

    // the second one should have moved
    val pos = pigs(1) !? GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 3, "incorrect: " + statusMap(pigs(1).port)) // assert the second pig moved from 3 -> 2
    println(statusMap)
    assert(statusMap(pigs(1).port) == false) // assert the second pig was not killed

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

    val statuses = ge.launch(target,Seq(Some(leader)), pigs, world)

    //    ge.stats(target, statuses, world)

    // we only have two pigs
    assert(statuses.size == 2)

    // the first one was trapped by the second pig
    assert(statuses(pigs(0).port))   // assert the first pig was hit

    // the second one should have moved
    val pos = pigs(1)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 2) // assert the second pig moved from 1 -> 2
    // ..and not been killed
    assert(statuses(pigs(1).port) == false) // assert the second pig was not killed

  }

  @Test
  def test4() = {
    Constants.DEBUG_MODE=true
    enableLogging(true)
    val numPigs = 3
    val (pigs, ports) = startPigs(numPigs)
    val part = partitionPigs(pigs, ports)
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
    val leaders = pigs.filter(_.amLeader).map(x => Some(x))

    println("Leaders are "+leaders(0).get.port+" and "+leaders(1).get.port)

    // Introduce the leaders:
    leaders(0).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(1).get.port)
    leaders(1).get !? SecondaryLeaderMessages.SecondaryLeader(leaders(0).get.port)


    leaders(0).get.cache
    leaders(1).get.cache

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

    val statuses = ge.launch(target,leaders.filterNot(x => (x == None || x.get.sleeping)), pigs, world)

    //    ge.stats(target, statuses, world)

    // we only have three pigs
    assert(statuses.size == 3)

    // the first one was trapped by the second pig
    var pos = pigs(0)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 0)   // assert the first pig didn't move
    assert(statuses(pigs(0).port))   // assert the first pig was hit

    // the second one was trapped by the third pig
    pos = pigs(1)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 1)   // assert the second pig didn't move
    assert(statuses(pigs(1).port))   // assert the second pig was hit

    // the third one should not have moved
    pos = pigs(2)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 2) // assert the third pig did not move
    // ..and not been killed
    assert(statuses(pigs(2).port) == false) // assert the third pig was not killed

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

    val statuses = ge.launch(target,Seq(Some(leader)), pigs, world)

    //    ge.stats(target, statuses, world)

    // we only have one pig
    assert(statuses.size == 1)

    // the first one should move 1 -> 2
    val pos = pigs(0)!?GetPosition() match { case Position(pos) => pos; case _ => -1}
    assert(pos == 2, "incorrect: " + statuses.head._1) //moved
    assert(statuses(pigs(0).port) == false)   // wasn't hit

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
