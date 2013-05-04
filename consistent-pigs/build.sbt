name := "consistent-pigs"

organization := "com.github.harshal.distos"

version := "1.0-SNAPSHOT"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "Sonatype" at "https://oss.sonatype.org/content/repositories",
  "repo.codahale.com" at "http://repo.codahale.com"
)

scalacOptions ++= Seq("-unchecked","-deprecation")

libraryDependencies ++= Seq(
    "com.codahale" % "logula_2.9.1" % "2.1.3",
    "junit" % "junit" % "4.10"
)

fork in run := true

fork in runMain := true

javaOptions in run += "-Xmx2G"

javaOptions in runMain += "-Xmx2G"
