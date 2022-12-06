package part2.structured.streaming

import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

object StreamingAggregation {

  LoggerFactory
    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    .asInstanceOf[Logger]
    .setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("StreamingAggregationApp")
    .master("local[2]")
    .getOrCreate()

  def linesCount() = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val linesCount = lines.select(col("value")).groupBy(col("value")).count()

    linesCount.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    linesCount()
  }

}
