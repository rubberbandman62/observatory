package observatory

import com.sksamuel.scrimage.{ Image, Pixel }
import org.apache.spark.sql.Dataset
import spark.implicits._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.commons.math3.util.FastMath._
import org.apache.spark.sql.Encoders
import org.apache.spark.storage.StorageLevel

/**
 * 2nd milestone: basic visualization
 */
object Visualization {

  val p = 4

  /**
   * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
   * @param location Location where to predict the temperature
   * @return The predicted temperature at `location`
   */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val p = 4
    val (n, d) = temperatures.par.aggregate((0.0d, 0.0d))((acc, locTemp) => {
      val gcDist = location.gcDistanceTo(locTemp._1)
      val weight = if (gcDist < 1.0d) {
        1000 - (gcDist * 1000).toInt
      } else 1 / pow(gcDist, p)
      (acc._1 + locTemp._2 * weight, acc._2 + weight)
    }, (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    n / d
  }

  def sparkPredictTemperature(averageTemperaturesDS: Dataset[AverageTemperature], location: Location): Temperature = {
    val inverseDistanceWeighting = new Aggregator[AverageTemperature, (Double, Double), Double] {
      def zero: (Double, Double) = (0.0d, 0.0d)
      def reduce(acc: (Double, Double), element: AverageTemperature) = {
        val distance = location.gcDistanceTo(Location(element.latitude, element.longitude))
        val weight = if (distance < 1.0d) {
          1000 - (distance * 1000).toInt
        } else 1 / pow(distance, p)

        (acc._1 + element.temperature * weight, acc._2 + weight)
      }
      def merge(b1: (Double, Double), b2: (Double, Double)) = (b1._1 + b2._1, b1._2 + b2._2)
      def finish(b: (Double, Double)) = b._1 / b._2
      override def bufferEncoder: Encoder[(Double, Double)] = Encoders.tuple(Encoders.scalaDouble, Encoders.scalaDouble)
      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }.toColumn

    averageTemperaturesDS
      .select(inverseDistanceWeighting)
      .collect()(0)
  }

  /**
   * @param points Pairs containing a value and its associated color
   * @param value The value to interpolate
   * @return The color that corresponds to `value`, according to the color scale defined by `points`
   */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    // find the closest temperatures t0 and t1 (with colors c0 and c1) next to value
    val (upper, lower) = points.foldLeft(((Double.MaxValue, Color(0, 0, 0)), (Double.MinValue, Color(0, 0, 0))))({
      case ((upper, lower), (temperature, color)) => {
        if (temperature >= value && temperature < upper._1)
          ((temperature, color), lower)
        else if (temperature <= value && temperature > lower._1)
          (upper, (temperature, color))
        else
          (upper, lower)
      }
    })

    if (lower._1 == Double.MinValue) upper._2
    else if (upper._1 == Double.MaxValue) lower._2
    else
      lower._2.interpolate(upper._2, (value - lower._1) / (upper._1 - lower._1))
  }

  def interpolateColorSimple(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    // find the closest temperatures t0 and t1 (with colors c0 and c1) next to value
    val (upper, lower) = points.foldLeft(((Double.MaxValue, Color(0, 0, 0)), (Double.MinValue, Color(0, 0, 0))))({
      case ((upper, lower), (temperature, color)) => {
        if (temperature >= value && temperature < upper._1)
          ((temperature, color), lower)
        else if (temperature <= value && temperature > lower._1)
          (upper, (temperature, color))
        else
          (upper, lower)
      }
    })

    if (lower._1 == Double.MinValue) upper._2
    else if (upper._1 == Double.MaxValue) lower._2
    else {
      val half = (upper._1 + lower._1) / 2
      val d = if (value < half) 0 else 1
      lower._2.interpolate(upper._2, d)
    }
  }

  /**
   * @param temperatures Known temperatures
   * @param colors Color scale
   * @return A 360×180 image where each pixel shows the predicted temperature at its location
   */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val width = 360
    val height = 180
    val alpha = 255

    val pixels = (0 until width * height).par.map(idx => {
      val location = new Location(idx % width, idx / width, width, height)
      val temperature = predictTemperature(temperatures, location)
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, alpha)
    })

    Image(width, height, pixels.toArray)
  }

  def visualizeDS(temperaturesDS: Dataset[AverageTemperature], colors: Iterable[(Temperature, Color)]): Image = {
    val width = 360
    val height = 180
    val alpha = 255

    val pixels = (0 until width * height).par.map(idx => {
      val location = new Location(idx % width, idx / width, width, height)
      val temperature = sparkPredictTemperature(temperaturesDS, location)
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, alpha)
    })

    Image(width, height, pixels.toArray)
  }

}

