#!/bin/sh
scala -cp classes:lib/netty-4.0.jar:lib/pickling-0.9.jar:lib/quasiquotes_2.10-2.0.0.jar silt.actors.DemoClient
