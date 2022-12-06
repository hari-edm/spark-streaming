package part2.structured.streaming

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{LoggerFactory}
import ch.qos.logback.classic.{Level, Logger}

object StreamingDataFrames {
  val logger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).
    asInstanceOf[Logger].setLevel(Level.INFO)

  val spark = SparkSession.builder().appName("FirstStreamingApp").master("local[2]").getOrCreate()

  def readFromSocket(): Unit = {

    val lines: DataFrame = spark.readStream.format("socket").option("host", "localhost").option("port", 12345).load()
    val query: StreamingQuery = lines.writeStream.format("console").outputMode("append").start()
    query.awaitTermination();

  }

  def main(args: Array[String]): Unit = {
    readFromSocket()
  }

}
