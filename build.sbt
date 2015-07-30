import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

lazy val buildSettings = Seq(
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.7"
)

lazy val `f-p` = (project in file(".")).
  settings(SbtMultiJvm.multiJvmSettings: _*).
  settings(buildSettings: _*).
  settings(
    name := "f-p",
    resolvers += Resolver.sonatypeRepo("snapshots"),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "spores-core" % "0.2.0-SNAPSHOT",
      "org.scala-lang.modules" %% "spores-pickling" % "0.2.0-SNAPSHOT",
      "io.netty" % "netty-all" % "4.0.30.Final",
      "com.typesafe.akka" % "akka-actor_2.11" % "2.3.12",
      "junit" % "junit-dep" % "4.11" % "test",
      "com.novocode" % "junit-interface" % "0.11" % "test"
    ),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s"),
    parallelExecution in Global := false,
    scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation"/*, "-Xprint:clean", "-Xlog-implicits"*/),
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test)
  ) configs (MultiJvm)

lazy val sample = Project(
  id = "sample",
  base = file("sample"),
  settings = buildSettings
) dependsOn(`f-p`)
