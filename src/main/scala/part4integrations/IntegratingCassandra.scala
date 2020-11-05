package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import common.{Car, carsSchema}
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}

object IntegratingCassandra {

  val spark=SparkSession.builder()
    .appName("integrating cassandra")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  def writeStreamToCassandraInBatches()={
    val carsDS=spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch{(batch:Dataset[Car],batchId:Long)=>
      //save this batch to cassandra in a single table write
        batch
          .select(col("Name"),col("Horsepower"))
          .write
          .cassandraFormat("cars","public")
          .mode(SaveMode.Append)
          .save()
    }
      .start()
      .awaitTermination()
  }
  class CassandraForeachWriter extends ForeachWriter[Car]{
    /*
    -on the every batch ,on every partition 'partitionId'
    -on every "epoch"= chunk of data
        -call the open method;if false, skip this chunk
        -for each entry in the chunk,call the process method
        -call the close method either at the end of the chunk or with an error if it was thrown
     */
    val keyspace="public"
    val table="cars"
    val connector=CassandraConnector(spark.sparkContext.getConf)
    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("open connection")
      true
    }

    override def process(car: Car): Unit ={
      connector.withSessionDo{session=>
        session.execute(
          s"""
            |insert into $keyspace.$table("Name","Horsepower")
            |values ('${car.Name}',${car.Horsepower.orNull})
            |""".stripMargin
        )
      }
    }
    override def close(errorOrNull: Throwable): Unit = println("closing connection")
  }
  def writeStreamToCassandra()={
    val carsDS=spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreach(new CassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
   writeStreamToCassandra()
    }
}
