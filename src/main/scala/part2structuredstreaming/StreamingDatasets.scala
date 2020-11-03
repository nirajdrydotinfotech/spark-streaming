package part2structuredstreaming
import common._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDatasets {

  val spark=SparkSession.builder()
    .appName("streaming datasets")
    .master("local[2]")
    .getOrCreate()

  //include encoders for DF->DS transformation
  import spark.implicits._

  def readCars():Dataset[Car]={
    //defining encoder explicitly useful for DF->DS transformation
  val carEncoder=Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load() //DF with a single column "value"
      .select(from_json(col("value"),carsSchema).as("car")) //composite column (struct)
      .selectExpr("car.*") //DF with multiple columns
      .as[Car]
    //.as[Car](carEncoder) //encoder can be passed implicitly with spark.implicits
  }

  def showCarNames()={
    val carsDS:Dataset[Car]=readCars()

    //transformation here
    val carNamesDf=carsDS.select(col("Name"))

    //collection transformation maintain type info
    val carNamesAlt:Dataset[String]=carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Exercise
    * 1)count how many powerful cars we have in the ds (HP>140)
    * 2)Average HP for entire DS
    * (use complete output mode)
    * 3)count the cars by origin field
    */
  def countCars()= {
    val carsDS = readCars()
      carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
        .awaitTermination()
    }
  def ex2()={
    val carsDS=readCars()
    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
    }
  def ex3()={
    val carsDS=readCars()
    val carsCountByOrigin=carsDS.groupBy(col("Origin")).count() //option  1
      val carsCountByOriginAlt=carsDS.groupByKey(car=>car.Origin).count() //option 2 with dataset api

    carsCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    ex3()
   }
}