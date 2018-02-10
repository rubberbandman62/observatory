package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.concurrent.TrieMap
import spark.implicits._

trait InteractionTest extends FunSuite with Checkers {

  test("InteractionTest#1: create the immages for the test year 2021") {

    import Extraction._
    import Interaction._
    import Manipulation._

    val stationsFile = "testStations1.csv"

    println()
    println("about to locate temperatures")

    val yearlyData = for {
      year <- 2021 to 2021
    } yield {
      val temperaturesFile = s"$year.csv"
      println("locate temperatures for: " + year)
      val locatedTemperatures = locateTemperatures(year, stationsFile, temperaturesFile)
      println("calculate averages for: " + year)
      val data = locationYearlyAverageRecords(locatedTemperatures)
      (year, data)
    }

    println("Now generating tiles")
    val t0 = System.nanoTime()
    generateTiles(yearlyData, generateImage)
    val t1 = System.nanoTime()
    println(s"It took ${(t1 - t0) / 1e9} seconds to build the tiles")
  }

  // The following takes very long and should not be run in a whole test suite:
  //
  //  test("InteractionTest#3: create the immages for the test year 2015") {
  //
  //    import Extraction._
  //    import Interaction._
  //    import Manipulation._
  //
  //    val stationsFile = "stations.csv"
  //    val stationsDS = loadStations(stationsFile)
  //
  //    val yearlyData = (for (year <- 2015 to 2015) yield {
  //      println()
  //      println(s"about to locate temperatures for $year")
  //      val temperaturesFile = s"$year.csv"
  //      println("locate temperatures for: " + year)
  //      val locatedTemperatures = sparkLocateTemperatures(year, stationsDS, temperaturesFile)
  //      println("calculate averages for: " + year)
  //      year -> sparkLocationYearlyAverages(locatedTemperatures).map(t => (t.location, t.temperature)).collect.toSeq
  //    }).toMap
  //
  //    println("Now generating tiles")
  //    val t0 = System.nanoTime()
  //    generateTiles(yearlyData, generateImage)
  //    val t1 = System.nanoTime()
  //    println(s"It took ${(t1 - t0) / 1e9} seconds to build the tiles")
  //
  //  }

}
