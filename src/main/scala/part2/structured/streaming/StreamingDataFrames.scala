package part2.structured.streaming

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}

import scala.concurrent.duration.DurationInt

object StreamingDataFrames {
  val logger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).
    asInstanceOf[Logger].setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("FirstStreamingApp").master("local[2]").getOrCreate()

  def readFromSocket(): Unit = {
    val lines: DataFrame = spark.readStream.format("socket").option("host", "localhost").option("port", 12345).load()
    val filteredDf: DataFrame = lines.filter(length(col("value")) > 5)
    val query: StreamingQuery = filteredDf.writeStream.format("console").outputMode("append").start()
    query.awaitTermination()
  }

  def readFromFile(): Unit = {
    val stocksSchema = StructType(Array(StructField("company", StringType, true),
      StructField("date", DateType, true),
      StructField("value", IntegerType, true)))

    val stocks = spark.readStream.format("csv").option("header", "false").option("dateFormat", "MMM d yyyy").schema(stocksSchema).load("hdfs://localhost:9000/user/dev/data/stocks/")
    stocks.writeStream.format("console").outputMode("append").start().awaitTermination();
  }

  def demoTrigger() = {
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 12345).option("dateFormat", "MMM d yyyy").load()

    lines.writeStream.format("console").outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime(2.seconds)).start().awaitTermination();
  }

  def main(args: Array[String]): Unit = {
    // readFromSocket()
    //readFromFile()
    demoTrigger()
  }

}
