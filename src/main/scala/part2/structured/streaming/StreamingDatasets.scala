package part2.structured.streaming

import ch.qos.logback.classic.{Level, Logger}
import common.Car
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

object StreamingDatasets {

  LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[Logger]
    .setLevel(Level.ERROR)
  def spark = SparkSession
    .builder()
    .appName("StreamingDatasetsApp")
    .master("local[2]")
    .getOrCreate()

  def readCar() = {
    val carEncoder = Encoders.product[Car]
    val carSchema = StructType(
      Array(
        StructField("Name", StringType, true),
        StructField("Miles_per_Gallon", DoubleType, true),
        StructField("Cylinders", LongType, true),
        StructField("Displacement", DoubleType, true),
        StructField("Horsepower", LongType, true),
        StructField("Weight_in_lbs", LongType, true),
        StructField("Acceleration", DoubleType, true),
        StructField("Year", StringType, true),
        StructField("Origin", StringType, true)
      )
    )
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carSchema).as("car"))
      .selectExpr("car.*")
      .as[Car](carEncoder)
  }
  def showCarNames() = {
    val carsDS: Dataset[Car] = readCar()

    val carNames = carsDS.map(_.Name)(Encoders.STRING)
    carNames.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    showCarNames
  }

}
