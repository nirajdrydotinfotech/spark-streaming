package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindow {

  val spark=SparkSession.builder()
    .appName("processing time windows")
    .master("local[*]")
    .getOrCreate()
  def aggregateByProcessingTime()={
    val linesCharCountByWinodwDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
      .select(col("value"),current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"),"10 seconds").as("window"))
      .agg(sum(length(col("value")))as("charCount")) //counting char every 10s by process time
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("charCount")
      )

      linesCharCountByWinodwDF.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
  aggregateByProcessingTime()
  }
}
