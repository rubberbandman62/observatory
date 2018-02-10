package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import Extraction._
import spark.implicits._
import org.apache.spark.sql.functions._
import java.time.LocalDate

trait ExtractionTest extends FunSuite {

  test("ExtractionTest#1: extend the file name") {
    val path1 = fsPath("testStations0.csv")
    val path2 = fsPath("/testStations0.csv")
    assert(path1.endsWith("\\test-classes\\testStations0.csv"))
    assert(path2.endsWith("\\test-classes\\testStations0.csv"))
  }

  test("ExtractionTest#2: import and clean stations") {
    val stations = "testStations0.csv"
    val stationsDS = loadStations(stations)

    assert(stationsDS.collect.size == 8, "8 stations should be loaded")
  }

  test("ExtractionTest#3: import and clean temperature measurements") {
    val year = 2020
    val measurementsDS = loadTemperaturesOfYear(year, year.toString() + ".csv")

    assert(measurementsDS.collect.size == 20, "20 measurments should be loaded")
  }

  test("ExtractionTest#4: join temperatures and measurements") {
    val year = 2020
    val stationsFile = "testStations0.csv"
    val temperaturesFile = year.toString() + ".csv"

    val stationsDS = loadStations(stationsFile)
    val locatedTemperatures = sparkLocateTemperatures(year, stationsDS, temperaturesFile)
    assert(locatedTemperatures.collect.size == 17, "17 joind records of temperatures and locations should be collected")
  }

  test("ExtractionTest#5: grader function should return an iterable") {
    val year = 2020
    val stationsFile = "testStations0.csv"
    val temperaturesFile = year.toString() + ".csv"

    val locatedTemperatures = locateTemperatures(year, stationsFile, temperaturesFile)
    assert(locatedTemperatures.size == 17, "17 joined records of temperatures and locations should be collected")
  }

  test("ExtractionTest#6: calculate the averages per year") {
    val year = 2020
    val stationsFile = "testStations0.csv"
    val temperaturesFile = "2020.csv"

    val stationsDS = loadStations(stationsFile)
    val locatedTemperatures = sparkLocateTemperatures(year, stationsDS, temperaturesFile)

    val averages = sparkLocationYearlyAverages(locatedTemperatures)

    val aver = averages.collect()
    aver.foreach(temp => {
      if (temp.latitude == 11.1)
        assert((temp.temperature * 100).toInt == -583)
      if (temp.latitude == 22.1)
        assert((temp.temperature * 100).toInt == -388)
      if (temp.latitude == 33.1)
        assert((temp.temperature * 100).toInt == -194)
      if (temp.latitude == 44.1)
        assert((temp.temperature * 100).toInt == 0)
      if (temp.latitude == 0.0)
        assert((temp.temperature * 100).toInt == 55)
      if (temp.latitude == 77.1)
        assert((temp.temperature * 100).toInt == 722)
      if (temp.latitude == 99.1)
        assert((temp.temperature * 100).toInt == 166)
    })

    assert(aver.size == 7, "There should 7 averages.")
  }

  test("ExtractionTest#7: grader function should return an iterable ") {
    val year = 2020
    val stationsFile = "testStations0.csv"
    val temperaturesFile = year.toString() + ".csv"

    val locatedTemperatures = locateTemperatures(year, stationsFile, temperaturesFile)
    val averages = locationYearlyAverageRecords(locatedTemperatures)
    assert(averages.size == 7, "7 avearges of temperatures should be collected")
  }

  test("ExtractionTest#8: measure the performance with the data for 2015") {
    val year = 2015
    val stationsFile = "Stations.csv"
    val temperaturesFile = year.toString() + ".csv"

    val t0 = System.nanoTime()

    val stationsDS = loadStations(stationsFile)
    val locatedTemperaturesDS = sparkLocateTemperatures(year, stationsDS, temperaturesFile)
    val averagesDS = sparkLocationYearlyAverages(locatedTemperaturesDS)
    val averages = averagesDS.collect()

    val t1 = System.nanoTime()
    val elapsedTimeInSeconds = (t1 - t0) / 1e9
    println
    println(s"It took $elapsedTimeInSeconds seconds to load ${averages.size} averages for year $year")
    assert(elapsedTimeInSeconds < 30.0d, "the averages for 2015 should be loaded in less than 20 seconds")
  }

  test("ExtractionTest#9: simulating grader error:'Array had size 7 instead of expected size 5'") {
    val locatedTemperatures = Array((LocalDate.of(2000, 1, 1), Location(1.0, -1.0), 10.0),
      (LocalDate.of(2000, 1, 2), Location(4.0, -4.0), 10.0),
      (LocalDate.of(2000, 1, 2), Location(2.0, -2.0), 10.0),
      (LocalDate.of(2000, 1, 3), Location(3.0, -3.0), 10.0),
      (LocalDate.of(2000, 1, 4), Location(4.0, -4.0), 10.0),
      (LocalDate.of(2000, 1, 4), Location(2.0, -2.0), 10.0),
      (LocalDate.of(2000, 1, 5), Location(5.0, -5.0), 10.0))
    val averages = locationYearlyAverageRecords(locatedTemperatures)
    assert(averages.size == 5, "6 avearges of temperatures should be collected")
  }

  test("ExtractionTest#10: simulating grader error:'Array had size 7 instead of expected size 5'") {
    val year = 2026
    val stationsFile = "testStations1.csv"
    val temperaturesFile = year.toString() + ".csv"

    val locatedTemperatures = locateTemperatures(year, stationsFile, temperaturesFile)
    locatedTemperatures.foreach(lt => println(s"${lt._1} ${lt._2} ${lt._3}"))
    val averages = locationYearlyAverageRecords(locatedTemperatures)
    averages.foreach(la => println(s"${la._1} ${la._2}"))
    assert(locatedTemperatures.size == 8, "8 located of temperatures should be collected")
    assert(averages.size == 6, "6 located of averages should be collected")
  }

  //  this test can only run alone otherwise an out of memory exception occurs:
  //
  //  test("ExtractionTest#11: grader test: locationYearlyAverageRecords should be able to process 1 million records") {
  //    val year = 2027
  //    val stationsFile = "testStations7.csv"
  //    val temperaturesFile = year.toString() + ".csv"
  //
  //    println("locate Temperatures")
  //    val locatedTemperatures = locateTemperatures(year, stationsFile, temperaturesFile)
  //    println(s"calculate averages after locating ${locatedTemperatures.size} temperatures")
  //    val averages = locationYearlyAverageRecords(locatedTemperatures)
  //    assert(averages.size > 2000, "2875 averages of temperatures should be collected")
  //  }

}