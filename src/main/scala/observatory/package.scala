import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

package object observatory {
  type Temperature = Double // °C, introduced in Week 1
  type Year = Int // Calendar year, introduced in Week 1

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("observatory")
    .getOrCreate()

  val epsilon = 1e-6
  val earthRadius = 6371.0 // kilometers

  val colorScale = Seq((60.0d, Color(255, 255, 255)),
    (0.0d, Color(0, 255, 255)),
    (12.0d, Color(255, 255, 0)),
    (32.0d, Color(255, 0, 0)),
    (-15.0d, Color(0, 0, 255)),
    (-50.0d, Color(33, 0, 107)),
    (-27.0d, Color(255, 0, 255)),
    (-60.0d, Color(0, 0, 0)))
    
}
