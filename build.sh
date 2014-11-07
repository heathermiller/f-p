#!/bin/sh
scalac -feature -d classes -cp lib/netty-4.0.jar:lib/pickling-0.9.jar:lib/quasiquotes_2.10-2.0.0.jar src/Scratch.scala src/SiloRef.scala src/SiloFactory.scala src/SiloSystem.scala src/SystemMessages.scala src/Builders.scala src/BuilderFactories.scala src/Place.scala src/Implicits.scala src/agents.scala src/RDD.scala src/package.scala src/netty/*.scala src/actors/*.scala
