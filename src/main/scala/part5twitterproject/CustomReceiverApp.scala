package part5twitterproject

import java.net.Socket

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.io.Source

class customSocketReceiver(host:String,port:Int) extends Receiver[String](StorageLevels.MEMORY_ONLY) {

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future

  //called asynchronously
  override def onStart(): Unit = {
    val socket = new Socket(host, port)

    //run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) //store make this string avilable for spark
    }
    socketPromise.success(socket)
  }

  //called asynchronously
  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}
object CustomReceiverApp {

  val spark=SparkSession.builder()
    .appName("Custom Receiver App")
    .master("local[*]")
    .getOrCreate()

  val ssc=new StreamingContext(spark.sparkContext,Seconds(1))

  def main(args: Array[String]): Unit = {
    val dataStream:DStream[String]=ssc.receiverStream(new customSocketReceiver("localhost",12345))
    dataStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
