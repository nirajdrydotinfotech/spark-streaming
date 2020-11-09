package part7practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Spark SQl
 */
object AllAboutScala2 extends App {

  val spark=SparkSession.builder()
    .appName("Spark SQL")
    .master("local[*]")
    .getOrCreate()

  val questionTagDF=spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/questions/question_tags_10K.csv")
    .toDF("id","tag")

  questionTagDF.createOrReplaceTempView("so_tags")

  val questionsCSVDF=spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/questions/questions_10K.csv")
    .toDF("id","creationDate","closedDate","deletionDate","score","ownerUserId","answerCount")

  val questionsDF=questionsCSVDF.select(
    col("id").cast("integer"),
    col("creationDate").cast("timestamp"),
    col("closedDate").cast("timestamp"),
    col("deletionDate").cast("date"),
    col("score").cast("integer"),
    col("ownerUserId").cast("integer"),
    col("answerCount").cast("integer")
  )
  val questionSubsetDF=questionsDF.filter("score > 400 and score < 410")

  questionSubsetDF.createOrReplaceTempView("so_questions")

  //list all table in spark's catalog
//  spark.catalog.listTables().show()

  //spark.sql("show tables").show()

  // spark.sql("select id ,tag from so_tags limit 10").show()

  spark.sql(
    "select * from so_tags where tag == 'php'")
    //.show()

    spark.sql(
      """select
        |count(*) as php_count
        |from so_tags where tag = 'php'
        |""".stripMargin
       )
      //.show()

  spark.sql(
    """select *
      |from so_tags
      |where tag like 's%'
      |""".stripMargin
  )
  //  .show()

  spark.sql(
    """select *
      |from so_tags
      |where tag like 's%'
      |and (id = 25 or id= 108)
      |""".stripMargin
  )
   // .show()

  spark.sql(
    """select *
      |from so_tags
      |where id in (25,108)
      |""".stripMargin)
  //  .show()

  spark.sql(
    """
      |select tag ,count(*) as count
      |from so_tags group by tag
      |""".stripMargin)
    //.show()

  spark.sql(
    """
      |select tag ,count(*) as count
      |from so_tags group by tag having count>5
      |""".stripMargin)
    //.show()

  spark.sql(
    """
      |select tag , count(*) as count
      |from so_tags group by tag having count>5 order by tag
      |""".stripMargin)
//    .show()

  spark.sql(
    """
      |select t.* , q.*
      |from so_questions q
      |inner join so_tags t
      |on t.id = q.id
      |""".stripMargin)
   // .show()

  spark.sql(
    """
      |select t.* , q.*
      |from so_questions q
      |left outer join so_tags t
      |on t.id = q.id
      |""".stripMargin)
 //   .show()

  spark.sql(
    """
      |select t.* , q.*
      |from so_questions q
      |right outer join so_tags t
      |on t.id = q.id
      |""".stripMargin)
 //   .show()

  spark.sql(
    """
      |select distinct tag from so_tags
      |""".stripMargin)
 //   .show()

  //function to prefix a string with so_ short for stackOverflow
  def prefixStackOverflow(s:String)=s"so_${s}"

  //register user define function(UDF)
  spark.udf.register("prefix_so",prefixStackOverflow _)

  //use udf prefix_so to augment each tag value with so_
  spark.sql(
    """
      |select id,
      |prefix_so(tag)
      |from so_tags
      |""".stripMargin)
    //.show()



}

