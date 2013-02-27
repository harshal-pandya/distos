import sbt._
import sbt.Keys._

object PigsBuild extends Build {

  lazy val pigs = Project(
    id = "pigs",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "Pigs",
      organization := "com.github.harshal.distos",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.1"
    )
  )
}
