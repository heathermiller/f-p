name := "f-p"

version := "0.1"

scalaVersion := "2.11.2"

libraryDependencies += "org.scala-lang" %% "scala-pickling" % "0.9.0"

libraryDependencies += "org.scala-lang.modules" %% "spores-core" % "0.1.0-SNAPSHOT"

libraryDependencies += "org.scala-lang.modules" %% "spores-pickling" % "0.1.0-SNAPSHOT"

libraryDependencies += "io.netty" % "netty-all" % "4.0.4.Final"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.3.6"

resolvers += Resolver.sonatypeRepo("snapshots")
 
scalacOptions ++= Seq("-unchecked", "-deprecation"/*, "-Xprint:clean", "-Xlog-implicits"*/)
