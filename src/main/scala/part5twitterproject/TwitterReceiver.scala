package part5twitterproject

import java.io.{OutputStream, PrintStream}

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

class TwitterReceiver extends Receiver[Status](StorageLevels.MEMORY_ONLY){

  val twitterStreamPromise: Promise[TwitterStream] =Promise[TwitterStream]
  val twitterStreamFuture: Future[TwitterStream] =twitterStreamPromise.future

  private def simpleStatusListener=new StatusListener {
    override def onStatus(status: Status): Unit = store(status)

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()

    override def onStallWarning(warning: StallWarning): Unit = ()

    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }
  private def redirectSystemError()=System.setErr(new PrintStream(new OutputStream {
    override def write(i: Int): Unit = ()
    override def write(b: Array[Byte]): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()

  }))

  override def onStart(): Unit = {
    redirectSystemError()
    val twitterStream=new TwitterStreamFactory("src/main/resources")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en")
    twitterStreamPromise.success(twitterStream)
  }

  override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
  twitterStream.cleanUp()
    twitterStream.shutdown()

  }
}
