package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    d00 * (1 - point.x) * (1 - point.y) +
    d10 * point.y * (1 - point.x) +
    d11 * point.x * point.y + 
    d01 * point.x * (1 - point.y)
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    import Visualization._
    val width = 256
    val transparency = 127
    val locations = tile.toListOfLocations(width)
    val pixels = locations.par.map(location => {
      val ((loc00, d00), (loc10, d10), (loc01, d01), (loc11, d11)) = Grid.findGridTemperatures(grid, location)
      val transformedLat = location.lat - loc00.lat
      val transformedLon = location.lon - loc00.lon
      val cp = CellPoint(abs(transformedLat - transformedLat.toInt), abs(transformedLon - transformedLon.toInt))
      val temp = bilinearInterpolation(cp, d00, d01, d10, d11)
      val color = interpolateColor(colors, temp)
      Pixel(color.red, color.green, color.blue, transparency)
    })

    Image(width, width, pixels.toArray)
  }

}
