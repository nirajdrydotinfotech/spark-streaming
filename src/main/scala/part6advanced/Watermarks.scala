package part6advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration.DurationInt

object Watermarks {

    val spark=SparkSession.builder()
      .appName("Watermark")
      .master("local[*]")
      .getOrCreate()

  def debugQuery(query:StreamingQuery)={
      //useful skill for debugging
    new Thread(()=>{
      (1 to 1000).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString
        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  import spark.implicits._
  def testWatermark()={
    val dataDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
      .as[String]
      .map{ line=>
        val token=line.split(",")
        val timeStamp=new Timestamp(token(0).toLong)
        val data=token(1)

        (timeStamp,data)
      }
      .toDF("created","color")

    val watermarkDF=dataDF
      .withWatermark("created","2 seconds")
      .groupBy(window(col("created"),"2 seconds"),col("color"))
      .count()
      .selectExpr("window.*","color","count")
  /*
  A 2 seconds watermark means
  -window will only be consider unitl watermark surpasses the window end
  -an element/row/record will be considered if after watermark
   */

    val query=watermarkDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      debugQuery(query)
      query.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
      testWatermark()
  }
}
object DataSender{
  val serverSocket=new ServerSocket(12345)
  val   socket=serverSocket.accept() //blocking call
  val printer=new PrintStream(socket.getOutputStream)

  println("socket accepted")
  def example1()={
     Thread.sleep(7000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red") //discarded:older than watermark
    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000,blue") //discarded:older than watermark
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("21000,green")
    Thread.sleep(3000)
    printer.println("4000,purple") //expect to be dropped-its older than watermark
    Thread.sleep(2000)
    printer.println("17000,green")
  }

  def example2()={
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")
    Thread.sleep(7000)
    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")
    Thread.sleep(3000)
    printer.println("10000,pink")
  }

  def example3()={
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red") // discarded
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green") // discarded
    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,green")
  }
  def main(args: Array[String]): Unit = {
    example1()
  }
}









