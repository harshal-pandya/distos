% CS677: Lab 1
% Harshal Pandya and Brian Martin

Our system represents a pig2pig network in which pigs collaborate to avoid
impact with an adversarial bird.

# Overall Design

The overall design uses an actor-based formalism.
TODO: more here

## Game Map

The game map is single-dimensional with pigs and columns placed randomly. It is
assumed that the bird is always launched from the left (as in the original
Angry Birds). The ratio of columns to pigs is at most one.

## Assumed Physics

We assume that certain behaviors for each element type:

  - **Pig**: An impacted pig will fall to the right (as the bird always
    approaches from the left). If a pig falls onto another pig both are
    considered impacted, but the second pig does not change position on impact.
    If a pig is impacted while having a column to the right, then that column
    will also fall to the right, affecting any pig which may find itself in
    that position.

  - **Column**: If a column is impacted directly, then it falls to the right,
    only affecting any pig or column to its immediate right.

## Game Engine

In our system the *game engine* has several roles:

  1. Map generation
  1. Pig network topology generation
  1. Sending round-initiating trajectory message to the nearest pig.
  1. Ends the round.


## Pigs as Actors

Each pig functions as an actor which can recieve and act on several message
types, derived from the original specification:

  - ``Trajectory(position: Int)``
  - ``BirdApproaching(targetPosition: Int, hopCount: Int)``
  - TODO: ``Status(pigId: Int)``
  - TODO: ``EndGame``
  - TODO: ``TakeShelter(??)``

## Launching a bird




# Description of "how it works"

# Design Decisions / Trade-offs

# Possible Improvements

    [and extensions]

   - Sketch how these would be done.

# How to run the program

We require ``maven``, the Java dependency management tool, to pull in all
dependencies and generate a jar file of compiled code.

We have included two scripts in the code directory:

  1. **``bin/install.sh``**: This will retrieve all the dependencies, compile the
     source, and produce a jar file.
  1. **``bin/run.sh``**: This will run a round of the game, starting several JVMs.

