package part7practice

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Dataframe operations
  */
object AllAboutScala4 extends App{

  val spark=SparkSession.builder()
    .appName("dataFrame Operations")
    .master("local[*]")
    .getOrCreate()

  val questionTagDF=spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/questions/question_tags_10K.csv")
    .toDF("id","tag")

  questionTagDF.show(10)

  val questionsCSVDF=spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/questions/questions_10K.csv")
    .toDF("id","creationDate","closedDate","deletionDate","score","ownerUserId","answerCount")

  val questionsDF=questionsCSVDF
    .filter("score > 400 and score < 410")
    .join(questionTagDF,"id")
    .selectExpr("ownerUserId","tag","creationDate","score")

  //questionsCSVDF.show()

  case class Tag(id:Int,tag:String)
import spark.implicits._
  val tagsDF=questionTagDF
    .as[Tag]
    .take(10)
    .foreach(t=>println(s"id = ${t.id}, tag= ${t.tag}"))

  val seqTags=Seq(
    1 -> "so_java",
    1->"so_jsp",
    2->"so_erlang",
    3->"so_scala",
    3->"so_akka"
  )
  val moreTagDF =seqTags.toDF("id","tag")
  //moreTagDF.show()

  val dfUnionOfTags=questionTagDF
    .union(moreTagDF)
    .filter("id in (1,3)")
    //dfUnionOfTags.show()

  val dgIntersectionTags=moreTagDF
    .intersect(dfUnionOfTags)
    //.show()

  val splitColumnDF=moreTagDF
    .withColumn("tmp",split($"tag","_"))
    .select(
      $"id",
      $"tag",
      $"tmp".getItem(0).as("so_prefix"),
      $"tmp".getItem(1).as("so_tag"),
    ).drop("tmp")
  splitColumnDF.show()

  val donuts=Seq(("plain donut",1.50),("vanilla donut",2.0),("glazed donut",2.50))
  val donutsDF=donuts.toDF("donut_name","donut_price")
  donutsDF.show()
}
