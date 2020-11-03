package part3lowlevel

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.sql.Date

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {
  val spark=SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()
      /*
      Spark Streaming Context = entry point to the DStreams API
      -need the spark context
      -a duration=batch interval
       */
  val ssc=new StreamingContext(spark.sparkContext,Seconds(1))
      /*
      -define input sources by creating DStreams
      -define transformations on DStreams
      -call an action on DStreams
      -start All computations with ssc.start()
          -no more computation can be added
      -await termination or stop the computation
          -you can not restart a computation
       */
  def readFromSocket()={
    val socketStream:DStream[String]=ssc.socketTextStream("localhost",12345)

    //transformation=lazy
    val wordsStream:DStream[String]=socketStream.flatMap(line=>line.split(" "))

    //action
    wordsStream.print()
  ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile()={
    new Thread(()=>{
      Thread.sleep(5000)

      val path="src/main/resources/data/stocks"
      val dir=new File(path) //directory where i will store new file
      val nFiles=dir.listFiles().length
      val newFiles=new File(s"$path/newStocks$nFiles.csv ")
      newFiles.createNewFile()

      val writer=new FileWriter(newFiles)
      writer.write(
        """
          |AAPL,Feb 1 2000,28.66
          |AAPL,Mar 1 2000,33.95
          |AAPL,Apr 1 2000,31.01
          |AAPL,May 1 2000,21
          |""".stripMargin.trim)
    writer.close()
    }).start()

  }
  def readFromFile()={
    createNewFile() //operates on another thread

    val stocksFilePath="src/main/resources/data/"
    /*
    ssc.textFileStream monitors a directory for new files
     */
    val textStream:DStream[String]=ssc.textFileStream(stocksFilePath)

    //transformation
    val dateFormat=new SimpleDateFormat("MMM d yyyy")
    val stocksStream:DStream[Stock]=textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company,date,price)
    }
    //action
    //stocksStream.print()
    stocksStream.saveAsTextFiles("src/main/resources/data/words/") //each folder =RDD=batch,each file=a partition of the rdd
    //start computation
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
readFromFile()
  }
}
