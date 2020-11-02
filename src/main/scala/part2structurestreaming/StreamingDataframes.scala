package part2structurestreaming

import common.stocksSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession, streaming}

import scala.concurrent.duration._

object StreamingDataframes {
  val spark=SparkSession.builder()
    .appName("streaming dataframes")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket()={
    //reading a df
    val lines:DataFrame = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
    //transformation
    val shortLines:DataFrame=lines.filter(length(col("value")) <= 5)

    //tell between a static vs streaming df
    println(shortLines.isStreaming)

    //consuming a df
    val query=lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    //wait for the stream to finish
     query.awaitTermination()
  }

  def readFromFiles()= {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
    //triggers
    def demoTriggers()={
      val lines:DataFrame = spark.readStream
        .format("socket")
        .option("host","localhost")
        .option("port",12345)
        .load()

    //write the line df at the certain trigger
      lines.writeStream
        .format("console")
        .outputMode("append")
        .trigger(
        //  Trigger.ProcessingTime(2.seconds) //every two seconds run the query
          //Trigger.Once() //single batch than terminate
        Trigger.Continuous(2.seconds) //experimental ,every 2 seconds create a batch with whatever you have
        )
        .start()
        .awaitTermination()
    }


  def main(args: Array[String]): Unit = {
 demoTriggers()
   }
}
