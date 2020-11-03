package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins {

  val spark=SparkSession.builder()
    .appName("Streaming joins")
    .master("local[2]")
    .getOrCreate()

  val guitarPlayers=spark.read
    .option("inferSchema",true)
    .json("src/main/resources/data/guitarPlayers")

  val guitar=spark.read
    .option("inferSchema",true)
    .json("src/main/resources/data/guitars")

  val bands=spark.read
    .option("inferSchema",true)
    .json("src/main/resources/data/bands")

  //joining static DFs
  val joinCondition=guitarPlayers.col("band") === guitarPlayers.col("id")
  val guitaristsBands=guitarPlayers.join(bands,joinCondition,"inner")
  val bandsSchema=bands.schema

  def joinStreamWithStatic()={
    val streamedBandsDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load() //a df with single column value
      .select(from_json(col("value"),bandsSchema).as("band"))
      .selectExpr("band.id as id","band.name as name","band.hometown as hometown","band.year as year")

    //joins happen per batch
    val streamBandsGuitarDF=streamedBandsDF
      .join(guitarPlayers,guitarPlayers
        .col("band") === streamedBandsDF.col("id"),"inner")

    /*
    restricted joins:
    -stream joining with static :RIGHT outer join/full outer join/right_semi not permitted
    -static joining with streaming :Left outer join/left_semi not permitted
     */
    streamBandsGuitarDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  //since spark 2.3 we have stream vs stream join
  def joinStreamWithStream()={
    val streamedBandsDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load() //a df with single column value
      .select(from_json(col("value"),bandsSchema).as("band"))
      .selectExpr("band.id as id","band.name as name","band.hometown as hometown","band.year as year")

    val streamedGuitaristsDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12346)
      .load() //a df with single column value
      .select(from_json(col("value"),guitarPlayers.schema ).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id","guitarPlayer.name as name","guitarPlayer.guitars as guitars","guitarPlayer.band as band")

    //joining streams with stream
    val streamJoins=streamedBandsDF.join(streamedGuitaristsDF,streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"))

    /*
    -inner join are supported
    -left/right join are supported ,but must have watermarks
    -full outer join are not supported
     */
    streamJoins.writeStream
      .format("console")
      .outputMode("append") //only append supported for stream vs stream join
      .start()
      .awaitTermination()
    }
  def main(args: Array[String]): Unit = {
        joinStreamWithStream()
  }
}
