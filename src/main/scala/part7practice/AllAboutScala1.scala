package part7practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * basic spark
  */
object AllAboutScala1 extends App {
  val spark=SparkSession.builder()
      .appName("allAboutScala practice")
      .master("local[*]")
      .getOrCreate()

  val questionTagDF=spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/questions/question_tags_10K.csv")
   .toDF("id","tag")

  val questionsCSVDF=spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/questions/questions_10K.csv")
    .toDF("id","creationDate","closedDate","deletionDate","score","ownerUserId","answerCount")

  //questionsDF.printSchema()
  //questionsDF.show()

  val questionsDF=questionsCSVDF.select(
    col("id").cast("integer"),
    col("creationDate").cast("timestamp"),
    col("closedDate").cast("timestamp"),
      col("deletionDate").cast("date"),
      col("score").cast("integer"),
      col("ownerUserId").cast("integer"),
      col("answerCount").cast("integer")
    )
  questionsDF.printSchema()

  questionsDF.show()

  questionTagDF.select("tag").show()

  questionTagDF.show(100)

  //print dataframe schema
  questionTagDF.printSchema()

  println(s"total rows og php is ${questionTagDF.filter("tag == 'php'").count()}")

  questionTagDF.filter("tag == 'php'").show()

  questionTagDF.filter("tag like 's%'")
   .filter("id ==25 or id == 108")
   .show(10)

  questionTagDF.filter("id in (25,108)").show()

  questionTagDF.groupBy("tag").count()
   .filter("count > 5")
   .orderBy("tag")
   .show()


  val questionSubsetDF=questionsDF.filter("score > 400 and score < 410")
  questionSubsetDF.show()

  questionSubsetDF.join(questionTagDF,"id")
    .select("ownerUserId","tag","creationDate","score")
    .show()

  questionSubsetDF.join(questionTagDF,questionTagDF("id")=== questionSubsetDF("id"))
      .show()

  questionSubsetDF.join(questionTagDF,Seq("id"),"inner")
   .show()

  questionSubsetDF.join(questionTagDF,Seq("id"),"left_outer")
  .show()

  questionSubsetDF.join(questionTagDF,Seq("id"),"right_outer")
    .show()

  questionTagDF.select("tag")
    .distinct()
    .show()

}
