package part2structuredstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StreamingAggregation {
  val spark=SparkSession.builder()
    .appName("streaming aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount()={
    val lines:DataFrame=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    val lineCount=lines.selectExpr("count(*) as lineCount")

    //aggregation with distinct are not supported
    //otherwise spark will need to keep track of everything

  lineCount.writeStream
    .format("console")
    .outputMode("complete")//append and update are not supported on aggregation without watermark
    .start()
    .awaitTermination()
  }

  def numericalAggregation(aggFunction:Column=>Column)={

    val lines:DataFrame=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    //aggregate here
    val numbers=lines
      .select(col("value")
        .cast("integer")
        .as("number"))

    val aggegationDF=numbers
      .select(aggFunction(col("number"))
        .as("agg_so_far"))

    aggegationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames()={
    val lines:DataFrame=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    //counting occurrences of the "name" value
    val names=lines
      .select(col("value").as("name"))
      .groupBy(col("name"))  //relationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
  def main(args: Array[String]): Unit = {
    groupNames()
  }
}
