package part3lowlevel

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

import common.Person
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamsTransformations {
  val spark = SparkSession.builder()
    .appName("DStreams transformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readPeople() = ssc.socketTextStream("localhost", 9999).map { line =>
    val tokens = line.split(":")
    Person(
      tokens(0).toInt, //id
      tokens(1), //fname
      tokens(2), // mname
      tokens(3), //lname
      tokens(4), //gender
      Date.valueOf(tokens(5)), //birth
      tokens(6), // ssn/uuid
      tokens(7).toInt //salary
    )
  }

  //map,flatmap,filter
  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  def peopleSmallNames(): DStream[String] = readPeople().flatMap{person =>
    List(person.firstName, person.middleName)

  }
  def highIncomePeople()=readPeople().filter(_.salary > 80000)

  //count
  def countPeople():DStream[Long]=readPeople().count()

    //count by value,per batch
  def countNames():DStream[(String,Long)]=readPeople().map(_.firstName).countByValue()

    /*reduce by key
  -works on DStreams of tuples
  -works per batch
  */
  def countNamesReduce():DStream[(String,Long)]=
    readPeople()
      .map(_.firstName)
      .map(name=>(name,1L))
      .reduceByKey((a,b)=>a+b)

  // foreach rdd transformation
  import spark.implicits._
  def saveToJson()=readPeople().foreachRDD{rdd=>
    val ds=spark.createDataset(rdd)
    val f=new File("src/main/resources/data/people")
    val nFiles=f.listFiles().length
    val path=s"src/main/resources/data/people/people$nFiles.json"
    ds.write.json(path)
  }

  def main(args: Array[String]): Unit = {
        saveToJson()

      ssc.start()
      ssc.awaitTermination()
    }
}
