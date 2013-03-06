package com.github.harshal.distos

import org.junit.Test
import org.junit.Assert._

@Test
class PigsTest {

  // One pig on a world of size one.
  @Test
  def test1() = {
    
    val numPigs = 1
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
    val ge = new GameEngine(numPigs, 2)
    val pigs = ge.generateTopology
    
    pigs(0).currentPos = 0
    pigs(1).currentPos = 2
    
    val world = Seq(
        Some(pigs(0)),
        Some(StoneColumn(1)),
        Some(pigs(1)),
        None)
    
    val target = 0
    
    val statuses = ge.launch(target, pigs, world)
    
    ge.stats(target, statuses, world)
    
    // we only have two pigs
    assert(statuses.size == 2) 
    
    // the first one was trapped by the column
    assert(statuses.head._2)   // assert the first pig was hit
    
    // the second one should have moved
    assert(statuses(1)._1.currentPos == 3) // assert the second pig moved from 3 -> 2
    assert(statuses(1)._2 == false) // assert the second pig was not killed
    
  }

}
