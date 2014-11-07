f-p
===

Open 2 terminals. In the 1st terminal we start the server:

    $ scala -cp classes:lib/netty-4.0.jar:lib/pickling-0.9.jar:lib/quasiquotes_2.10-2.0.0.jar silt.netty.Server 8090

In the 2nd terminal we start the client:

    $ scala -cp classes:lib/netty-4.0.jar:lib/pickling-0.9.jar:lib/quasiquotes_2.10-2.0.0.jar silt.netty.Client
