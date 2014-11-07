name := "f-p"

version := "0.1"

scalaVersion := "2.11.2"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "org.scala-lang" %% "scala-pickling" % "0.9.0"

libraryDependencies += "org.scala-lang.modules" %% "spores-core" % "0.1.0-SNAPSHOT"

libraryDependencies += "org.scala-lang.modules" %% "spores-pickling" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.netty" % "netty-all" % "4.0.4.Final"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.3.6"

libraryDependencies += "junit" % "junit-dep" % "4.10" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s")

parallelExecution in Global := false

scalacOptions ++= Seq("-unchecked", "-deprecation"/*, "-Xprint:clean", "-Xlog-implicits"*/)
