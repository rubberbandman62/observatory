package observatory

import java.io.BufferedWriter
import java.io.FileWriter

import org.apache.log4j.Level
import org.apache.log4j.Logger

import observatory.Extraction.sparkLocateTemperatures
import observatory.Extraction.sparkLocationYearlyAverages
import java.nio.file.Paths
import java.io.BufferedReader
import java.io.FileReader
//import au.com.bytecode.opencsv.CSVReader
//import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConverters

import spark.implicits._

object Main extends App {

//  import Extraction._
//  import Interaction._
//  import Manipulation._
//  import Visualization2._
//
//  def fsPath(file: String): String = {
//    val file_name = if (file.startsWith("/")) file else "/" + file
//    Paths.get(getClass.getResource(file_name).toURI()).toString()
//  }
//
//  def openFile(folder: String, name: String): java.io.File = {
//    val dir = new java.io.File(folder)
//    if (!dir.exists())
//      dir.mkdirs()
//    val filename = s"$folder/$name"
//    new java.io.File(filename)
//  }
//
//  def openCSVOutputFile(folder: String, name: String): CSVWriter = {
//    val file = openFile(folder, name)
//    val outputFile = new BufferedWriter(new FileWriter(file))
//    new CSVWriter(outputFile)
//  }
//
//  def openCSVInputFile(folder: String, name: String): CSVReader = {
//    val file = openFile(folder, name)
//    val inputFile = new BufferedReader(new FileReader(file))
//    new CSVReader(inputFile)
//  }
//
//  def isNormalDataAvailable: Boolean = {
//    val filename = "target/normals/normaldata.csv"
//    (new java.io.File(filename)).exists()
//  }
//
//  def saveNormalData(yearlyData: Iterable[(Int, Seq[(Location, Temperature)])]): Unit = {
//    val folder = s"target/normals"
//    val filename = s"normaldata.csv"
//    val csvWriter = openCSVOutputFile(folder, filename)
//    val listOfRecords = new ArrayList[Array[String]]()
//    for {
//      (year, data) <- yearlyData
//      locationData <- data
//    } listOfRecords.add(Array(year.toString, locationData._1.lat.toString, locationData._1.lon.toString, locationData._2.toString))
//    csvWriter.writeAll(listOfRecords)
//    csvWriter.close()
//  }
//
//  def loadNormalData(): Iterable[(Int, Seq[(Location, Temperature)])] = {
//    val folder = s"target/normals"
//    val filename = s"normaldata.csv"
//    val csvReader = openCSVInputFile(folder, filename)
//    val listOfRecords = csvReader.readAll()
//    csvReader.close()
//    val records = JavaConverters.asScalaIteratorConverter(listOfRecords.iterator()).asScala.toSeq
//    val mappedRecords = (for (record <- records) yield (record(0).toInt, (Location(record(1).toDouble, record(2).toDouble), record(3).toDouble))).groupBy(_._1)
//    val result = for ((key, value) <- mappedRecords) yield (key, value.map(_._2))
//    result.toMap
//  }
//
//  def isAverageNormalsGridAvailable: Boolean = {
//    val filename = "target/normals/normalgrid.csv"
//    (new java.io.File(filename)).exists()
//  }
//
//  def saveAverageNormalGrid(grid: GridLocation => Temperature): Unit = {
//    val folder = s"target/normals"
//    val filename = s"normalgrid.csv"
//    val csvWriter = openCSVOutputFile(folder, filename)
//    val listOfRecords = new ArrayList[Array[String]]()
//    for {
//      gl <- Grid.gridLocations
//    } listOfRecords.add(Array(gl.lat.toString, gl.lon.toString, grid(gl).toString))
//    csvWriter.writeAll(listOfRecords)
//    csvWriter.close()
//  }
//
//  def loadAverageNormalGrid(): Grid = {
//    val folder = s"target/normals"
//    val filename = s"normalgrid.csv"
//    val csvReader = openCSVInputFile(folder, filename)
//    val listOfRecords = csvReader.readAll()
//    csvReader.close()
//    val records = JavaConverters.asScalaIteratorConverter(listOfRecords.iterator()).asScala.toSeq
//    val mappedRecords = Map(records.map(record => GridLocation(record(0).toInt, record(1).toInt) -> record(2).toDouble): _*)
//    Grid.restore(mappedRecords)
//  }
//
//  def generateImageForDeviations(year: Year, tile: Tile, data: GridLocation => Temperature): Unit = {
//    val colorScale = Seq(
//      (7.0d, Color(0, 0, 0)),
//      (4.0d, Color(255, 0, 0)),
//      (2.0d, Color(255, 255, 0)),
//      (0.0d, Color(255, 255, 255)),
//      (-2.0d, Color(0, 255, 255)),
//      (-7.0d, Color(0, 0, 255)))
//
//    generateImageMain(year, tile, colorScale, data)
//  }
//
//  def generateImageForTemperatures(year: Year, tile: Tile, data: GridLocation => Temperature): Unit = {
//    val colorScale = Array(
//      (60.0d, Color(255, 255, 255)),
//      (32.0d, Color(255, 0, 0)),
//      (12.0d, Color(255, 255, 0)),
//      (0.0d, Color(0, 255, 255)),
//      (-15.0d, Color(0, 0, 255)),
//      (-27.0d, Color(255, 0, 255)),
//      (-50.0d, Color(33, 0, 107)),
//      (-60.0d, Color(0, 0, 0)))
//
//    generateImageMain(year, tile, colorScale, data)
//  }
//
//  def generateImageMain(year: Year, tile: Tile, colorScale: Seq[(Temperature, Color)], data: GridLocation => Temperature): Unit = {
//    val t0 = System.nanoTime()
//    val image = visualizeGrid(data, colorScale, tile)
//    val folderYearZoom = s"target/deviations/$year/${tile.zoom}"
//    val filename = s"${tile.x}-${tile.y}.png"
//    val file = openFile(folderYearZoom, filename)
//    image.output(file)
//    val t1 = System.nanoTime()
//    println(s"image $filename took ${((t1 - t0) * 10000 / 1e9).toInt / 10000.0d} seconds to generate.")
//  }
//
//  val stationsFile = "stations.csv"
//  val stationsDS = loadStations(stationsFile)
//
//  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//
//  println()
//  println("calculate yearly averages for the years 1975 to 1990:")
//  val t0 = System.nanoTime()
//  val normalData = if (isNormalDataAvailable) {
//    loadNormalData()
//  } else {
//    val newNormalData = (for {
//      year <- 1975 to 1990
//    } yield {
//      val temperaturesFile = s"$year.csv"
//      val locatedTemperatures = sparkLocateTemperatures(year, stationsDS, temperaturesFile)
//      (year, sparkLocationYearlyAverages(locatedTemperatures).map(t => (t.location, t.temperature)).collect.toSeq)
//    }).toMap
//    saveNormalData(newNormalData)
//    println("normal data has been saved to file")
//    newNormalData
//  }
//  val t1 = System.nanoTime()
//  println()
//  println(s"Calculatig averages for the years 1975 to 1990 took ${((t1 - t0) / 1e6).toInt / 1000.0d} seconds.")
//
//  normalData.foreach({ case (year, data) => println(s"$year => ${data.size} records") })
//
//  println()
//  println("Now generating average grid for normal temperatures calculated from the years 1975 to 1990")
//  val t2 = System.nanoTime()
//  val averageNormalsGrid = if (isAverageNormalsGridAvailable) {
//    val ng = loadAverageNormalGrid()
//    ng.get _
//  } else {
//    val newAverageNormalsGrid = average(normalData.map(_._2))
//    saveAverageNormalGrid(newAverageNormalsGrid)
//    println("normal grid has been saved to file")
//    newAverageNormalsGrid
//  }
//  val t3 = System.nanoTime()
//  println()
//  println(s"Creating average grid for the years 1975 to 1990 took ${((t3 - t2) / 1e6).toInt / 1000.0d} seconds.")
//
//  println()
//  println("Now visualize normal temperatures as year 1990")
//  val t31 = System.nanoTime()
//  generateTiles(Map(1990 -> averageNormalsGrid), generateImageForTemperatures)
//  val t32 = System.nanoTime()
//  println()
//  println(s"Visualizing normal temperatures took ${((t32 - t31) / 1e6).toInt / 1000.0d} seconds.")
//
//  println()
//  println("Calculating deviation for the years 1991 to 2015")
//  val t4 = System.nanoTime()
//  for (year <- 1997 to 2015) yield {
//    val t41 = System.nanoTime()
//    val temperaturesFile = s"$year.csv"
//    val locatedTemperatures = sparkLocateTemperatures(year, stationsDS, temperaturesFile)
//    val averageTemperaturesForYear = sparkLocationYearlyAverages(locatedTemperatures).map(t => (t.location, t.temperature)).collect.toSeq
//    val t42 = System.nanoTime()
//    println()
//    println(s"Creating averages for $year took ${((t42 - t41) / 1e6).toInt / 1000.0d} seconds.")
//    val gridsPerYear = Map(year -> deviation(averageTemperaturesForYear, averageNormalsGrid))
//    val t43 = System.nanoTime()
//    println()
//    println(s"Creating grid for $year took ${((t42 - t41) / 1e6).toInt / 1000.0d} seconds.")
//
//    val t6 = System.nanoTime()
//    generateTiles(gridsPerYear, generateImageForDeviations)
//    val t7 = System.nanoTime()
//    println()
//    println(s"Creating all the images for $year took ${((t7 - t6) / 1e6).toInt / 1000.0d} seconds.")
//  }
//  val t5 = System.nanoTime()
//  println()
//  println(s"Creating all the grids and images for 1991 to 2015 took ${((t5 - t4) / 1e6).toInt / 1000.0d} seconds.")
//
//  spark.stop()
}
