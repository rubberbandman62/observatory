package observatory

import java.time.LocalDate
import java.nio.file.Paths
import org.apache.spark.sql.Dataset
import spark.implicits._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConverters._

/**
 * 1st milestone: data extraction
 */
object Extraction {

  def fsPath(file: String): String = {
    val file_name = if (file.startsWith("/")) file else "/" + file
    Paths.get(getClass.getResource(file_name).toURI()).toString()
  }

  def loadStations(filename: String): Dataset[Station] =
    spark.read
      .schema(StructType(Array(StructField("stationId", StringType),
        StructField("wbanId", StringType),
        StructField("latitude", DoubleType),
        StructField("longitude", DoubleType))))
      .csv(fsPath(filename))
      .na.drop(Array("latitude", "longitude"))
      .na.fill("", Seq("stationId", "wbanId"))
      .filter((trim($"stationId") =!= "") || (trim($"wbanId") =!= ""))
      .select(trim($"stationId").as("stationId"), trim($"wbanId").as("wbanId"), $"latitude", $"longitude")
      .as[Station]

  def loadTemperaturesOfYear(year: Int, filename: String): Dataset[Measurement] =
    spark.read
      .schema(StructType(Array(StructField("stationId", StringType),
        StructField("wbanId", StringType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("inFahrenheit", DoubleType))))
      .csv(fsPath(filename))
      .na.drop(Array("month", "day", "inFahrenheit"))
      .na.fill("", Seq("stationId", "wbanId"))
      .select(trim($"stationId").as("stationId"),
        trim($"wbanId").as("wbanId"),
        lit(year).as("year"),
        $"month",
        $"day",
        (($"inFahrenheit" - 32) / 1.8d).as("temperature"))
      .filter(($"stationId" =!= "") || ($"wbanId" =!= ""))
      .as[Measurement]

  /**
   * @param year             Year number
   * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
   * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
   * @return A sequence containing triplets (date, location, temperature)
   */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsDS = loadStations(stationsFile)
    val locatedTemperatures = sparkLocateTemperatures(year, stationsDS, temperaturesFile)
    val locatedTemperaturesIterable = (for (month <- 1 to 12) yield {
      locatedTemperatures
        .filter($"month" === month)
        .collect
        .map(lt => (lt.date, lt.location, lt.temperature))
    }).flatten
    locatedTemperaturesIterable
  }

  def sparkLocateTemperatures(year: Year, stationsDS: Dataset[Station], temperaturesFile: String): Dataset[LocatedTemperature] = {
    val measurements = loadTemperaturesOfYear(year, temperaturesFile)
    measurements.join(stationsDS, Seq("stationId", "wbanId"))
      .select($"year", $"month", $"day", $"latitude", $"longitude", $"temperature")
      .as[LocatedTemperature]
  }

  /**
   * @param records A sequence containing triplets (date, location, temperature)
   * @return A sequence containing, for each location, the average temperature over the year.
   */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] =
    spark.sparkContext.parallelize(records.toSeq, 8)
      .map({ case (localdate, location, temp) => (location, (temp, 1)) })
      .reduceByKey((value1, value2) => (value1._1 + value2._1, value1._2 + value2._2), 4)
      .map({ case (key, value) => (key, value._1 / value._2) })
      .collect

  def sparkLocationYearlyAverages(locatedTemperatures: Dataset[LocatedTemperature]): Dataset[AverageTemperature] = {
    val selected = locatedTemperatures.select($"latitude", $"longitude", $"temperature")
    val grouped = selected.groupBy($"latitude", $"longitude")
    grouped.agg(avg("temperature").alias("temperature"))
      .as[AverageTemperature]
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

}
