package baby_spark

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random
import silt._
import silt.actors._

import scala.spores._
import scala.pickling._
import Defaults._
import SporePickler._

/**
  * Replication of http://mbrace.io/starterkit/HandsOnTutorial/examples/200-kmeans-clustering-example.html
  * using F-P
  */

object KMeansHelper {
  type Point = Array[Double]

  val seed = 42
  val dim = 2
  val numCentroids = 5
  val partitions = 12
  val pointsPerPartition = 10000
  val epsilon = 0.1

  def printMatrix(mat: Array[Array[Double]]): String = {
    mat.map(_.mkString(", ")).mkString("\n")
  }

  def dist(p1: Point, p2: Point): Double = p1.zip(p2).foldLeft(0.0)({
    case (acc, (e1, e2)) => acc + Math.pow(e1 - e2, 2)
  })

  def findCentroid(p: Point, centroids: Array[Point]): Int = {
    // Find the closest centroid, return its index
    var mini = 0
    var min = Double.MaxValue

    for (i <- 0 until centroids.length) yield {
      val d = dist(p, centroids(i))
      if (d < min) {
        min = d
        mini = i
      }
    }

    mini
  }

  def kmeansLocal(points: Array[Point], centroids: Array[Point]): Array[(Int, (Int, Point))] = {
    val lens = Array.ofDim[Int](centroids.length)
    val sums = Array.ofDim[Double](centroids.length, centroids(0).length)

    for (point <- points) yield {
      val cent = findCentroid(point, centroids)
      lens(cent) = lens(cent) + 1
      for (i <- 0 until point.length) yield {
        sums(cent)(i) = sums(cent)(i) + point(i)
      }
    }

    (for (i <- 0 until centroids.length) yield (i, (lens(i), sums(i)))).toArray
  }

  def sumPoints(pointArr: Array[Point]): Point = {
    val sum = Array.ofDim[Double](dim)
    for (p <- pointArr; i <- 0 until dim) {
      sum(i) = sum(i) + p(i)
    }
    sum
  }

  def divPoint(point: Point, x: Float): Point = point.map(_/x)
}

object KMeans {
  import KMeansHelper._

  def generatePoints(dim: Int, numPoints: Int, seed: Long): Array[Point] = {
    val rand = new Random
    rand.setSeed(seed * 2003 + 22)
    val prev = new Point(dim)

    def nextPoint(): Point = {
      val arr = new Point(dim)
      for (i <- 0 to dim - 1) yield {
        arr(i) = prev(i) + rand.nextFloat() * 40 - 20
        prev(i) = arr(i)
      }
      arr
    }

    (for (_ <- 1 to numPoints) yield nextPoint ()).toArray
  }

  def kMeansIterate(
    partitionedPoints: Seq[SiloRef[Array[Point]]], centroids: Array[Point],
    iteration: Int
  ): Array[Point] = {
    println(s"Running iteration $iteration...")

    val clusterParts = partitionedPoints.map(silo => {
      silo.apply(spore {
        val lCentroids = centroids
        points => kmeansLocal(points, lCentroids)
      }).send()
    })

    val newCentroids = Await.result(Future.sequence(clusterParts).map(seq => {
      seq
        .reduce(_ ++ _)
        .groupBy(_._1)
        .toSeq
        .sortBy(_._1)
        .map(_._2)
        .map(clp => clp.map(_._2).toArray.unzip)
        .map({ case (ns, points) => ns.sum -> sumPoints(points) })
        .map({ case (n, sum) => divPoint(sum, n) })
    }), 10.seconds).toArray

    val diff = newCentroids.zip(centroids).map({ case (p1, p2) => dist(p1, p2) }).max

    println(s"KMeans: iteration $iteration, diff $diff with centroids\n${printMatrix(newCentroids)}")

    if (diff < epsilon) {
      newCentroids
    } else {
      kMeansIterate(partitionedPoints, newCentroids, iteration + 1)
    }
  }

  def kMeansCloud(points: Array[Array[Point]], system: SystemImpl, hosts: Seq[Host]): Array[Point] = {
    val initCentroids = points.flatten.take(numCentroids)

    println("KMeans: persisting partitioned point to store.")

    val partitionedPointsFutures = Future.sequence(points.zipWithIndex.map { case (content, i) => {
      system.fromFun(hosts(i))(spore {
        val lPoints = content
        _ => new LocalSilo[Array[Point]](lPoints)
      })
    }}.toSeq)

    val partitionedPoints = Await.result(partitionedPointsFutures, Duration.Inf).toSeq
    println("KMeans: persist completed, starting iteration.")

    kMeansIterate(partitionedPoints, initCentroids, 1)
  }

  def main(args: Array[String]): Unit = {
    val randPoints = for (_ <- 1 to partitions) yield { generatePoints (dim, pointsPerPartition, seed) }

    val system = new SystemImpl
    val started = system.start(partitions)
    val hosts = for (i <- 0 to partitions) yield { Host("127.0.0.1", 8090 + i)}

    kMeansCloud(randPoints.toArray, system, hosts)

    system.waitUntilAllClosed(1.hour, 1.hour)
  }
}
