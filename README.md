# Function Passing: Typed, Distributed Functional Programming

[![Build Status](https://travis-ci.org/heathermiller/f-p.svg?branch=master)](https://travis-ci.org/heathermiller/f-p/)

The `F-P` library uses [sbt](http://www.scala-sbt.org/) for building and
testing. Distributed tests and examples are supported using the
[sbt-multi-jvm](https://github.com/sbt/sbt-multi-jvm) sbt plug-in.

Homepage: [http://lampwww.epfl.ch/~hmiller/f-p](http://lampwww.epfl.ch/~hmiller/f-p)

## How to Run Examples and Tests

Examples and tests can be launched from the sbt prompt:

```
> project samples
> [info] Set current project to f-p samples (in build file:/.../f-p/)
> run
> [warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list
>
> Multiple main classes detected, select one to run:
>
>  [1] samples.datastructure.Demo
>  [2] samples.getstarted.Client
>  [3] samples.getstarted.Server
>
> Enter number:
>
> project core
> multi-jvm:run netty.Basic
> multi-jvm:run netty.WordCount
```

Note, `samples.getstarted.Client` and `samples.getstarted.Server` must be run in different `sbt` sessions, e.g., in two different Terminals. 
