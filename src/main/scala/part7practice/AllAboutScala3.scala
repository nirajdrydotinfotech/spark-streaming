package part7practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * DataFrame statistics
  */
object AllAboutScala3 extends App {

  val spark=SparkSession.builder()
    .appName("dataframe statistics")
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

  val questionsDF=questionsCSVDF.select(
    col("id").cast("integer"),
    col("creationDate").cast("timestamp"),
    col("closedDate").cast("timestamp"),
    col("deletionDate").cast("date"),
    col("score").cast("integer"),
    col("ownerUserId").cast("integer"),
    col("answerCount").cast("integer")
  )

  questionsDF.select(avg("score"))
    //.show()

  questionsDF.select(max("score"))
   // .show()

  questionsDF.select(min("score"))
    //.show()

  questionsDF.select(mean("score"))
    //.show()

  questionsDF.select(sum("score"))
   // .show()

  questionsDF.filter("id > 400 and id < 450")
    .filter("ownerUserId is not null")
    .join(questionTagDF,"id")
    .groupBy("ownerUserId")
    .agg(avg("score"),max("answerCount"))
  //.show()

  val questionsStatisticsDF=questionsDF.describe()
  //questionsStatisticsDF.show()

  val correlation=questionsDF.stat.corr("score","answerCount")
  println(s"correlation between score and answer count is $correlation")

  val covariance=questionsDF.stat.cov("score","answerCount")
  println(s"covariance between score anf answer count os $covariance")

  //questionsDF.stat.freqItems(Seq("answerCount")).show()

    val scoreByUserId=questionsDF
      .filter("ownerUserId > 0 and ownerUserId < 20")
      .stat
      .crosstab("score","ownerUserId")
      //.show()

  //find all rows where answerCount in (5,10,20)
  val questionByAnswerCountDF=questionsDF
    .filter("ownerUserId > 0")
    .filter("answerCount in (5,10,20)")

  //count how many rows match in answerCount in (5,10,20)
  questionByAnswerCountDF
    .groupBy("answerCount")
    .count()
    .show()

  //create a fraction map where we are only interested:
  // - 50% of the rows that have answer_count = 5
  // - 10% of the rows that have answer_count = 10
  // - 100% of the rows that have answer_count = 20
  // Note also that fractions should be in the range [0, 1]

  val fractionMap=Map(5 -> 0.5,10 -> 0.1 ,20 -> 1.0)

  //stratified sample using the fractionKeyMap
  questionByAnswerCountDF.stat
    .sampleBy("answerCount",fractionMap,37L)
    .groupBy("answerCount")
    .count()
    .show()

  // Approximate Quantile
  val quantiles = questionsDF
    .stat
    .approxQuantile("score", Array(0, 0.5, 1), 0.25)
  println(s"Qauntiles segments = ${quantiles.toSeq}")

  questionsDF.createOrReplaceTempView("so_questions")
  spark
    .sql("select min(score), percentile_approx(score, 0.25), max(score) from so_questions")
    .show()

  //bloom filter
  val tagBloomFilter=questionTagDF.stat.bloomFilter("tag",1000L,0.1)
  println(s"bloom filter contains java tag = ${tagBloomFilter.mightContain("java")}")
  println(s"bloom filter contains some unknown tag = ${tagBloomFilter.mightContain("unknown tag")}")

  // Count Min Sketch
  val cmsTag = questionTagDF.stat.countMinSketch("tag", 0.1, 0.9, 37)
  val estimatedFrequency = cmsTag.estimateCount("java")
  println(s"Estimated frequency for tag java = $estimatedFrequency")

  // Sampling With Replacement
  val dfTagsSample = questionTagDF.sample(true, 0.2, 37L)
  println(s"Number of rows in sample dfTagsSample = ${dfTagsSample.count()}")
  println(s"Number of rows in dfTags = ${questionTagDF.count()}")



}
