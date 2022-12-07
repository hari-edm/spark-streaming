package part2.structured.streaming

import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

object StreamingJoins {

  LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[Logger]
    .setLevel(Level.ERROR)
  val spark = SparkSession
    .builder()
    .appName("SparkJoinApp")
    .master("local[2]")
    .getOrCreate()

  val guiter = spark.read
    .option("inferSchema", true)
    .json("hdfs://localhost:9000/user/dev/data/guitars/")
  val guiterPlayers = spark.read
    .option("inferSchema", true)
    .json("hdfs://localhost:9000/user/dev/data/guitarPlayers/")

  def joinStreamWithStatic() = {
    val bandsSchema = StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, true),
        StructField("hometown", StringType, true),
        StructField("year", IntegerType, true)
      )
    )
    val bandStreamDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr(
        "band.id as band_id",
        "band.name as band_name",
        "band.hometown as hometown",
        "band.year as year"
      )

    val joinedDf = bandStreamDf
      .join(
        guiterPlayers,
        bandStreamDf.col("band_id") === guiterPlayers.col("band"),
        "inner"
      )
      .select(
        col("id"),
        col("name").as("player_name"),
        col("band_id"),
        col("band_name")
      )

    joinedDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    joinStreamWithStatic
  }

}
