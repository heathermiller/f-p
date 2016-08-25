import sbt._
import Keys._

import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

object build extends Build {
  type Sett = Def.Setting[_]

  lazy val standardSettings: Seq[Sett] = Seq[Sett](
    scalaVersion := "2.11.7",
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked"
    ),
    resolvers ++= (if (version.value.endsWith("-SNAPSHOT")) List(Resolver.sonatypeRepo("snapshots")) else Nil),
    parallelExecution in Global := false,
    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s"),
    unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_)),
    unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))
  )

  lazy val `f-p` = Project(
    id = "f-p",
    base = file("."),
    settings = standardSettings,
    aggregate = Seq(core, samples)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = standardSettings ++ SbtMultiJvm.multiJvmSettings ++ Seq[Sett](
      name := "f-p core",
      libraryDependencies ++= Seq(
        "org.scala-lang.modules" %% "spores-core"     % "0.2.0",
        "org.scala-lang.modules" %% "spores-pickling" % "0.2.0",
        "io.netty" % "netty-all" % "4.0.30.Final",
        "com.typesafe.akka" % "akka-actor_2.11" % "2.3.12",
        "com.typesafe.scala-logging" %% "scala-logging"   % "3.1.0",
        "ch.qos.logback"              % "logback-classic" % "1.1.3",
        "junit"        % "junit-dep"       % "4.10" % "test",
        "com.novocode" % "junit-interface" % "0.11" % "test"
      ),
      compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test)
    )
  ) configs (MultiJvm)

  lazy val samples = Project(
    id = "samples",
    base = file("samples"),
    settings = standardSettings ++ Seq[Sett](
      name := "f-p samples"
    )
  ) dependsOn(`core`)

  lazy val baby_spark = Project(
    id = "baby_spark",
    base = file("baby_spark"),
    settings = standardSettings ++ SbtMultiJvm.multiJvmSettings ++ Seq[Sett](
      name := "baby_spark",
      fork := true,
      libraryDependencies ++= Seq(
        "org.scalaz" %% "scalaz-core" % "7.2.0"
      ),
      cancelable in Global := true,
      compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test)
    )
  ) dependsOn(core) configs (MultiJvm)

}
