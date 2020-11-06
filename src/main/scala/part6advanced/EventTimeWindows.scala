package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows {

        val spark=SparkSession.builder()
          .appName("Event Time Windows")
          .master("local[*]")
          .getOrCreate()

        val onlinePurchaseSchema=StructType(Array(
          StructField("id",StringType),
          StructField("time",TimestampType),
          StructField("item",StringType),
          StructField("quantity",IntegerType)
          ))

    def readPurchasesFromFile()=spark
      .readStream
      .schema(onlinePurchaseSchema)
      .json("src/main/resources/data/purchases")

    def readPurchasesFromSocket()=spark
      .readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
      .select(from_json(col("value"),onlinePurchaseSchema).as("purchase"))
      .selectExpr("purchase.*")

    def aggregatePurchaseByTumblingWindow()={
      val purchaseDF=readPurchasesFromSocket()
        val windowByDay=purchaseDF
        .groupBy(window(col("time"),"1 day") //tumbling window :sliding duration=window duration
          .as("time"))  //struct column has two field :start,end
        .agg(sum("quantity").as("totalQuantity"))
        .select(
          col("time").getField("start"),
          col("time").getField("end"),
          col("totalQuantity")
      )

      windowByDay.writeStream
      .format("console")
      .outputMode("complete")
        .start()
      .awaitTermination()
    //    windowByDay.show(20, false)

  }
    def aggregatePurchaseBySlidingWindow()={
      val purchaseDF=readPurchasesFromSocket()
        val windowByDay=purchaseDF
        .groupBy(window(col("time"),"1 day","1 hour")
          .as("time"))  //struct column has two field :start,end
        .agg(sum("quantity").as("totalQuantity"))
        .select(
          col("time").getField("start"),
          col("time").getField("end"),
          col("totalQuantity")
        )
      windowByDay.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()

      //windowByDay.show(30, false)
    }

  /**
    * Exercises
    * show me the best selling product from everyday,+quantity
    * show best selling product of every 24 hrs,updated every hour
    * */

    def bestSellingProduct()={
      val purchaseDF=readPurchasesFromFile()
        val windowByDay=purchaseDF
        .groupBy(col("item"),window(col("time"),"1 day")
          .as("day"))  //struct column has two field :start,end
        .agg(sum("quantity").as("totalQuantity"))
        .select(
          col("day").getField("start"),
          col("day").getField("end"),
        col("item"), col("totalQuantity")
        )
          .orderBy(col("day"),col("totalQuantity").desc)

      windowByDay.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()
    }

  def bestSellingProductByHour()={
    val purchaseDF=readPurchasesFromFile()
    val windowByHour=purchaseDF
      .groupBy(col("item"),window(col("time"),"1 day","1 hour")
      .as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      ).orderBy(col("start"),col("totalQuantity").desc)

    windowByHour.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregatePurchaseBySlidingWindow()
  }
}
