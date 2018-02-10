package observatory

import java.time.LocalDate
import org.apache.commons.math3.util.FastMath._

/**
 * Introduced in Week 1. Represents a location on the globe.
 * @param lat Degrees of latitude, -90 ≤ lat ≤ 90
 * @param lon Degrees of longitude, -180 ≤ lon ≤ 180
 */
case class Location(lat: Double, lon: Double) {
  lazy val phi = toRadians(lat)
  lazy val lambda = toRadians(lon)
  lazy val sinPhi = sin(phi)
  lazy val cosPhi = cos(phi)

//  lazy val antipode =
//    Location(-this.lat, if (allmostEqual(this.lon, 0.0d)) 180 else signum(this.lon) * (-1) * (180 - abs(this.lon)))

  private def allmostEqual(x: Double, y: Double): Boolean =
    abs(x - y) < epsilon

//  def isAntipodeOf(that: Location): Boolean =
//    this.antipode == that

  def gcDistanceTo(that: Location): Double = {
//    if (this.isAntipodeOf(that))
//      earthRadius * PI
//    else {
      val deltaRho = acos(sinPhi * that.sinPhi + cosPhi * that.cosPhi * cos(abs(this.lambda - that.lambda)))
      earthRadius * deltaRho
   }

  def toImageCoordinates(width: Int = 360, height: Int = 180): (Int, Int) =
    (((lon + 180) * width / 360).toInt, ((90 - lat) * height / 180).toInt)

  def this(x: Int, y: Int, width: Int = 360, height: Int = 180) =
    this(90 - (y / height.toDouble * 180.0d), (x / width.toDouble * 360.0d) - 180)

}

/**
 * Introduced in Week 3. Represents a tiled web map tile.
 * See https://en.wikipedia.org/wiki/Tiled_web_map
 * Based on http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
 * @param x X coordinate of the tile
 * @param y Y coordinate of the tile
 * @param zoom Zoom level, 0 ≤ zoom ≤ 19
 */
case class Tile(x: Int, y: Int, zoom: Int) {
  lazy val toLocation = new Location(
    toDegrees(atan(sinh(PI * (1.0 - 2.0 * y.toDouble / (1 << zoom))))),
    x.toDouble / (1 << zoom) * 360.0 - 180.0)

  def toURI = new java.net.URI("http://tile.openstreetmap.org/" + zoom + "/" + x + "/" + y + ".png")

  private def toListOfRowsOfLocations(level: Int): Seq[Seq[Location]] = {
    if (zoom + level >= 19)
      Seq(Seq(this.toLocation, this.toLocation), Seq(this.toLocation, this.toLocation))
    else {
      val tileUpperLeft = Tile(x * 2, y * 2, zoom + 1)
      val tileUpperRight = Tile(x * 2 + 1, y * 2, zoom + 1)
      val tileLowerLeft = Tile(x * 2, y * 2 + 1, zoom + 1)
      val tileLowerRight = Tile(x * 2 + 1, y * 2 + 1, zoom + 1)

      if (level <= 1) {
        Seq(Seq(tileUpperLeft.toLocation, tileUpperRight.toLocation),
          Seq(tileLowerLeft.toLocation, tileLowerRight.toLocation))
      } else {
        val leftList = tileUpperLeft.toListOfRowsOfLocations(level - 1) ++
          tileLowerLeft.toListOfRowsOfLocations(level - 1)

        val rightList = tileUpperRight.toListOfRowsOfLocations(level - 1) ++
          tileLowerRight.toListOfRowsOfLocations(level - 1)

        leftList.zip(rightList).map({ case (l, r) => l ++ r })
      }
    }
  }

  def toListOfLocations(width: Int): Seq[Location] = {
    val exp = (log10(width) / log10(2.0)).toInt
    this.toListOfRowsOfLocations(exp).foldLeft(Seq[Location]())((z, l) => z ++ l)
  }

}

case class Grid(temperatures: Iterable[(Location, Temperature)]) {
  private val data = new Array[Temperature](Grid.dataSize)
  private val dataExists = new Array[Boolean](Grid.dataSize)

  def get(gl: GridLocation): Temperature = {
    val idx = Grid.index(gl.lat, gl.lon)
    if (dataExists(idx))
      data(idx)
    else 
      put(gl, Visualization.predictTemperature(temperatures, Location(gl.lat, gl.lon)))
  }

  def put(gl: GridLocation, temp: Temperature): Temperature = {
    val idx = Grid.index(gl.lat, gl.lon)
    data(idx) = temp
    dataExists(idx) = true
    temp
  }

}

object Grid {
  val resolution: Int = 2
  val width: Int = 360
  val height: Int = 180

  private val dataSize = (width / resolution) * (height / resolution)

  private val minLon = -width / 2
  private val maxLon = minLon + width - 1
  private val minLat = -height / 2 + 1
  private val maxLat = minLat + height - 1

  def index(lat: Int, lon: Int) =
    -(lat / resolution - maxLat / resolution) * width / resolution + lon / resolution - minLon / resolution

  def restore(restoredData: Map[GridLocation, Temperature]): Grid = {
    val newGrid = new Grid(Seq())
    restoredData.foreach({ case (gl, temp) => newGrid.put(gl, temp) })
    newGrid
  }

  lazy val gridLocations = {
    for {
      lat <- maxLat to minLat by -resolution
      lon <- minLon to maxLon by resolution
    } yield GridLocation(lat, lon)
  }

  def findGridTemperatures(g: GridLocation => Temperature, loc: Location): ((GridLocation, Temperature), (GridLocation, Temperature), (GridLocation, Temperature), (GridLocation, Temperature)) = {
    val top = scala.math.ceil(loc.lat).toInt
    val left = scala.math.floor(loc.lon).toInt
    val bottom = top - 1
    val right = if (left < maxLon) left + 1 else minLon
    val glTopLeft = GridLocation(top, left)
    val glTopRight = GridLocation(top, right)
    val glBottomRight = GridLocation(bottom, right)
    val glBottomLeft = GridLocation(bottom, left)
    ((glBottomLeft, g(glBottomLeft)),
      (glBottomRight, g(glBottomRight)),
      (glTopLeft, g(glTopLeft)),
      (glTopRight, g(glTopRight)))
  }

}

//case class Grid(val temperatures: Iterable[(Location, Temperature)]) {
//
//  private val data = HashMap[GridLocation, Temperature]()
//  val iterator = data.iterator
//
//  def get(gridLocation: GridLocation): Double = {
//    val dlat = abs(gridLocation.lat % Grid.resolution)
//    val dlon = abs(gridLocation.lon % Grid.resolution)
//    val lat = gridLocation.lat + dlat
//    val lon = gridLocation.lon + dlon
//    val key = GridLocation(lat, lon)
//    data.getOrElse(key, {
//      val temp = Visualization.predictTemperature(temperatures, Location(lat, lon))
//      data.put(key, temp)
//      temp
//    })
//  }
//
//  def this(restoredData: Map[GridLocation, Temperature]) = {
//    this(restoredData.toIterable.map({ case (gl, temp) => (Location(gl.lat, gl.lon), temp) }))
//    restoredData.foreach({ case (gl, temp) => this.data.put(gl, temp) })
//  }
//}
//
//object Grid {
//  val resolution = 2
//
//  def findGridTemperatures(g: GridLocation => Temperature, loc: Location): ((GridLocation, Temperature), (GridLocation, Temperature), (GridLocation, Temperature), (GridLocation, Temperature)) = {
//    val top = scala.math.ceil(loc.lat).toInt
//    val left = scala.math.floor(loc.lon).toInt
//    val bottom = top - 1
//    val right = if (left < maxx) left + 1 else minx
//    val glTopLeft = GridLocation(top, left)
//    val glTopRight = GridLocation(top, right)
//    val glBottomRight = GridLocation(bottom, right)
//    val glBottomLeft = GridLocation(bottom, left)
//    ((glBottomLeft, g(glBottomLeft)),
//      (glBottomRight, g(glBottomRight)),
//      (glTopLeft, g(glTopLeft)),
//      (glTopRight, g(glTopRight)))
//  }
//
//  lazy val gridLocations = {
//    for {
//      lat <- maxy to miny by -resolution
//      lon <- minx to maxx by resolution
//    } yield GridLocation(lat, lon)
//  }
//
//  val width = 360
//  val height = 180
//  val minx = -width / 2
//  val maxx = minx + width - 1
//  val miny = -height / 2 + 1
//  val maxy = miny + height - 1
//  val dataSize = (width / resolution) * (height / resolution)
//}

/**
 * Introduced in Week 4. Represents a point on a grid composed of
 * circles of latitudes and lines of longitude.
 * @param lat Circle of latitude in degrees, -89 ≤ lat ≤ 90
 * @param lon Line of longitude in degrees, -180 ≤ lon ≤ 179
 */
case class GridLocation(lat: Int, lon: Int)

/**
 * Introduced in Week 5. Represents a point inside of a grid cell.
 * @param x X coordinate inside the cell, 0 ≤ x ≤ 1
 * @param y Y coordinate inside the cell, 0 ≤ y ≤ 1
 */
case class CellPoint(x: Double, y: Double)

/**
 * Introduced in Week 2. Represents an RGB color.
 * @param red Level of red, 0 ≤ red ≤ 255
 * @param green Level of green, 0 ≤ green ≤ 255
 * @param blue Level of blue, 0 ≤ blue ≤ 255
 */
case class Color(red: Int, green: Int, blue: Int) {
  def interpolate(that: Color, weight: Double): Color = {
    // assert(weight >= 0.0d && weight <= 1.0d)
    val r: Double = round(red + weight * (that.red - red))
    val g: Double = round(green + weight * (that.green - green))
    val b: Double = round(blue + weight * (that.blue - blue))
    Color(r.toInt, g.toInt, b.toInt)
  }

  def distance(that: Color): Double = {
    sqrt((this.red - that.red) * (this.red - that.red) +
      (this.green - that.green) * (this.green - that.green) +
      (this.blue - that.blue) * (this.blue - that.blue))
  }
}

case class Station(stationId: String, wbanId: String, latitude: Double, longitude: Double)

case class Measurement(stationId: String, wbanId: String, year: Int, month: Int, day: Int, temperature: Double)

case class LocatedTemperature(year: Int, month: Int, day: Int, latitude: Double, longitude: Double, temperature: Temperature) {
  val date: LocalDate = LocalDate.of(year, month, day)
  val location: Location = Location(latitude, longitude)
}

case class AverageTemperature(latitude: Double, longitude: Double, temperature: Double) {
  val location: Location = Location(latitude, longitude)
}
