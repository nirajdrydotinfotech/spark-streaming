package part4integrations

import common._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object IntegratingKafka {

  val spark=SparkSession.builder()
    .appName("Integrating kafka")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka()={

    val kafkaDF:DataFrame=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","rockthejvm")
      .load()
    kafkaDF.select(col("topic"),expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka()={
    val carsDF=spark.readStream
        .schema(carsSchema)
        .json("src/main/resources/data/cars")
    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key","Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","rockthejvm")
      .option("checkpointLocation","checkpoints") //without checkpoints the writing to kafka will fail
      .start()
      .awaitTermination()
  }

  /**
    * Exercise:write the whole cars data structured to kafka as JSON.
    * use struct columns an the to _json function.
    */
  def writeCarsToKafka()={
    val carsDF=spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF=carsDF.select(
      col("Name").as("key"),
      to_json(struct(
        col("Name"),
        col("Cylinders"),col("Displacement"),
        col("Horsepower"),col("Weight_in_lbs"),
        col("Acceleration"),col("Year"),
        col("Origin"))).cast("String").as("value"))

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","rockthejvm")
      .option("checkpointLocation","checkpoints")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
  writeCarsToKafka()
  }
}
