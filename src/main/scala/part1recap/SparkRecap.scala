package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRecap {

  //the entry point to the spark structured api
  val spark=SparkSession.builder()
    .appName("spark Recap")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  //reads a DF
  val carsDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars")
  //select
  val usefulCarsData=carsDF.select(
    col("Name"), //column object
    $"Year" //another column object(needs spark implicits)
      (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWeights=carsDF.selectExpr("Weight_in_lbs / 2.2")

  //filter
  val europeanCars=carsDF.filter(col("Origin") =!= "USA") ///.where(col("Origin") =!= "USA")

  //aggregation
  val averageHP=carsDF.select(avg(col("Horsepower")).as("avg_HP"))
  //sum,mean,stddev,min,max etc

  //grouping
  val countByOrgin=carsDF.groupBy(col("Origin")) //a relational grouped Dataset
    .count()

  //joining
  val guitarPlayersDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitarPlayers")

  val bandsDF=spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/bands")

  val guitaristBands=guitarPlayersDF.join(bandsDF,
    guitarPlayersDF.col("band") === bandsDF.col("id"))
  /*
  join types
  -inner:only the matching rows are kept
  -left/right/ful outer join
   -semi join/anti join
   */
  //datasets=typed distributed collection of object
  case class GuitarPlayer(id:Long,name:String,guitar:Seq[Long],band:Long)
  val guitarPlayerDS=guitarPlayersDF.as[GuitarPlayer] //needs spark implicit
  guitarPlayerDS.map(_.name)

  //spark sql
  carsDF.createOrReplaceTempView("cars")
  val americanCars=spark.sql(
    """
      |select name from cars where Origin='USA'
      |
      |""".stripMargin
  )
  //low level API:RDDs
  val sc=spark.sparkContext
  val numbersRDD: RDD[Int] =sc.parallelize(1 to 100000)

  //functional operators
  val doubles=numbersRDD.map(_*2)
  //RDD->DF
  val numbersDF=numbersRDD.toDF("numbers") //you loose type info,you get SQL capability

  //RDD ->DS
  val numbersDS=spark.createDataset(numbersRDD)

  //DS->RDD
  val guitarPlayerRDD=guitarPlayerDS.rdd

  //DF->RDD
  val carsRDD=carsDF.rdd //RDD[ROWS]


  def main(args: Array[String]): Unit = {
    //showing a DF to the console
    carsDF.show()
    carsDF.printSchema()
  }

}
