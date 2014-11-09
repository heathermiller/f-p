
lazy val root = (project in file(".")).
  settings(
    name := "f-p",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.11.4",
    resolvers += Resolver.sonatypeRepo("snapshots"),
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-pickling" % "0.10.0",
      "org.scala-lang.modules" %% "spores-core" % "0.1.1-SNAPSHOT",
      "org.scala-lang.modules" %% "spores-pickling" % "0.1.1-SNAPSHOT",
      "io.netty" % "netty-all" % "4.0.4.Final",
      "com.typesafe.akka" % "akka-actor_2.11" % "2.3.6",
      "junit" % "junit-dep" % "4.10" % "test",
      "com.novocode" % "junit-interface" % "0.10" % "test"
    ),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v", "-s"),
    parallelExecution in Global := false,
    scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation"/*, "-Xprint:clean", "-Xlog-implicits"*/)
  )
