package observatory

import com.sksamuel.scrimage.{ Image, Pixel, ScaleMethod }
import Visualization._
import org.apache.spark.sql.Dataset
import spark.implicits._

/**
 * 3rd milestone: interactive visualization
 */
object Interaction {

  /**
   * @param tile Tile coordinates
   * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
   */
  def tileLocation(tile: Tile): Location =
    tile.toLocation

  /**
   * @param temperatures Known temperatures
   * @param colors Color scale
   * @param tile Tile coordinates
   * @return A 256×256 image showing the contents of the given tile
   */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val scale = 2
    val width = 256 / scale
    val height = width
    val transparency = 127
    val pixels = tile.toListOfLocations(width).par.map(location => {
      val temperature = predictTemperature(temperatures, location)
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, transparency)
    })
    Image(width, height, pixels.toArray).scale(scale, ScaleMethod.Bilinear)
  }

  def tileDS(temperaturesDS: Dataset[AverageTemperature], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val scale = 2
    val width = 256 / scale
    val height = width
    val transparency = 127
    val temperatures = temperaturesDS.map(t => (t.location, t.temperature)).collect
    val pixels = tile.toListOfLocations(width).par.map(location => {
      val temperature = predictTemperature(temperatures, location)
      val color = interpolateColor(colors, temperature)
      Pixel(color.red, color.green, color.blue, transparency)
    })
    Image(width, height, pixels.toArray).scale(scale, ScaleMethod.Bilinear)
  }

  /**
   * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
   * @param yearlyData Sequence of (year, data), where `data` is some data associated with
   *                   `year`. The type of `data` can be anything.
   * @param generateImage Function that generates an image given a year, a zoom level, the x and
   *                      y coordinates of the tile and the data to build the image from
   */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit): Unit = {
    for {
      (year, data) <- yearlyData
      zoom <- 0 to 3
      x <- 0 until 1 << zoom
      y <- 0 until 1 << zoom
    } generateImage(year, Tile(x, y, zoom), data)
  }

  def generateImage[Data <: Iterable[(Location, Temperature)]](year: Year, t: Tile, data: Data): Unit = {
    val t0 = System.nanoTime()
    val image = tile(data, colorScale, t)
    val folderYearZoom = s"target/temperatures/$year/${t.zoom}"
    val dir = new java.io.File(folderYearZoom)
    if (!dir.exists())
      dir.mkdirs()
    val filename = s"$folderYearZoom/${t.x}-${t.y}.png"
    val file = new java.io.File(filename)
    image.output(file)
    val t1 = System.nanoTime()
    println(s"tile $filename took ${((t1 - t0) * 10000 / 1e9).toInt / 10000.0d} seconds to generate.")
  }
}
