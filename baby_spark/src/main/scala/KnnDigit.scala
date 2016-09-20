package baby_spark

import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Random
import silt._
import silt.actors._

import scala.spores._
import scala.pickling._
import Defaults._
import SporePickler._

/**
  * Taken and replicated from
  * http://mbrace.io/starterkit/HandsOnTutorial/examples/200-knn-digit-recognizer-example.html
  */

object Image {
  def parse(path: String): Vector[Image] = {
    Source.fromFile(path)
      .getLines.drop(1)
      .map(_.split(',').map(_.toLong))
      .zipWithIndex.map({
        case (nums, i) => Image(i + 1, nums)
      })
      .toVector
  }
}

case class Image(id: KnnDigitHelper.ImageId, pixels: Array[Long])

object TrainingImage {
  def parse(path: String): Vector[TrainingImage] = {
    Source.fromFile(path)
      .getLines.drop(1)
      .map(_.split(',').map(_.toInt))
      .zipWithIndex.map({
        case (nums, i) => {
          val id = i + 1
          val image = Image(id, nums.slice(1, nums.length).map(_.toLong))
          TrainingImage(nums(0), image)
        }
      })
      .toVector
  }
}

case class TrainingImage(classification: KnnDigitHelper.Classification, image: Image)

object KnnDigitHelper {
  type ImageId = Int
  type Classification = Int
  type Distance = (Image, Image) => Long
  type Classifier = (Vector[TrainingImage], Image) => Classification

  val pixelLength = 784

  val nActors = 10


  def l2: Distance = {
    def f(x: Image, y: Image): Long = {
      val xp = x.pixels
      val yp = y.pixels
      var acc = 0L
      for (i <- 0 until pixelLength) yield {
        acc = acc + Math.pow(xp(i) - yp(i), 2).toLong
      }
      acc
    }
    f
  }

  def knn(d: Distance, k: Int): Classifier = {
    def f(training: Vector[TrainingImage], img: Image): Classification = {
      training
        .sortBy(m => d(m.image, img))
        .take(k)
        .map(_.classification)
        .groupBy(x => x).mapValues(_.length)
        .reduce[(Int, Int)]({
          case (m1, m2) => {
            if (m1._2 >= m2._2) m1
            else m2
          }
        })._2
    }
    f
  }

  def partition[T](arr: Vector[T], partitions: Int): Vector[Vector[T]] = {
    arr
      .zipWithIndex.groupBy({ case (a, i) => i % partitions })
      .map(_._2.map(_._1))
      .toVector
  }

  def classify(classifier: Classifier, training: Vector[TrainingImage], images: Vector[Image]): Vector[(ImageId, Classification)] = {
    images.par.map(img => img.id -> classifier(training, img)).toVector
  }

  def validate(classifier: Classifier, training: Vector[TrainingImage], validation: Vector[TrainingImage]): Double = {
    validation.par
      .map(tr => tr.classification -> classifier(training, tr.image))
      .map({ case (expected, prediction) => if (expected == prediction) 1.0 else 0.0 })
      .sum
  }
}

object KnnDigit {
  import KnnDigitHelper._

  def createSplittedSilos[T, V](
    n: Int, full: Vector[T], splitted: Vector[V],
      hosts: Seq[Host], system: SystemImpl): Vector[SiloRef[(Vector[T], Vector[V])]] = {

    val split = partition(splitted, n)
    val siloF = Future.sequence(split.zip(hosts).map {
      case (tr, host) => {
        system.fromFun(host)(spore {
          val lFull = full
          val lTr = tr
          _ => new LocalSilo[(Vector[T], Vector[V])](lFull -> lTr)
        })
      }
    }.toSeq)

    return Await.result(siloF, Duration.Inf).toVector
  }

  def main(args: Array[String]): Unit = {
    val system = new SystemImpl
    val started = system.start(nActors)
    val hosts = for (i <- 0 to nActors) yield { Host("127.0.0.1", 8090 + i)}

    val cwd = new java.io.File(".").getCanonicalPath
    println(s"Current dir = ${cwd}")

    val trainPath = "data/train.csv"
    val testPath = "data/test.csv"

    val training = TrainingImage.parse(trainPath)
    val tests = Image.parse(testPath)

    val classifier = knn(l2, 10)

    val validateSplit = 40000
    val validateSilos = createSplittedSilos(nActors, training.slice(0, validateSplit),
      training.slice(validateSplit, training.length), hosts, system)
    val classifySilos = createSplittedSilos(nActors, training, tests, hosts, system)

    val validateResF = validateSilos.map {
      silo => {
        silo.apply(spore {
          val lClass = classifier
          content => validate(lClass, content._1, content._2)
        }).send()
      }
    }

    val valResult = Await.result(Future.sequence(validateResF)
      .map(f => {
        f.sum / validateSplit
      }), Duration.Inf)

    println(s"Validation result: ${valResult}")

    // val classifyResF = classifySilos.map {
    //   silo => {
    //     silo.apply(spore {
    //       val lClass = classifier
    //       content => classify(lClass, content._1, content._2)
    //     }).send()
    //   }
    // }

    // val pred = Await.result(Future.sequence(classifyResF), Duration.Inf)

    system.waitUntilAllClosed(1.hour, 1.hour)
  }
}
