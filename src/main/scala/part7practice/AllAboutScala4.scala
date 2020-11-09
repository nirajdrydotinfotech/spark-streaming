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

   // questionTagDF.show(10)

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
    //splitColumnDF.show()

    val donuts=Seq(("plain donut",1.50),("vanilla donut",2.0),("glazed donut",2.50))
    val donutsDF=donuts.toDF("donut_name","donut_price")
  //  donutsDF.show()

   val (columnNames,columnDataTypes)=donutsDF.dtypes.unzip
    println(s"column names : ${columnNames.mkString(",")}")
    println(s"column types : ${columnDataTypes.mkString(",")}")

    val donuts1 = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))

    val donuts1DF=donuts1.toDF("id","donut_name","donut_price")
    //donuts1DF.show()

    val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
    val inventoryDF=inventory.toDF("id","inventory")
    //inventoryDF.show()

    val donutsInventoryDF=inventoryDF.join(donuts1DF,Seq("id"),"inner")

    //donutsInventoryDF.show()

    val donuts2DF=donutsDF.withColumn("Tasty",lit(true))
      .withColumn("Correlation",lit(1))
      .withColumn("Stock Min Max",typedLit(Seq(100,500)))

    donuts2DF.show()

    val stockMinMax:String=>Seq[Int]={
      case "plain donut" => Seq(100, 500)
      case "vanilla donut" => Seq(200, 400)
      case "glazed donut" => Seq(300, 600)
      case _ => Seq(150, 150)
    }

    val udfStockMinMax=udf(stockMinMax)
    val df2=donutsDF.withColumn("stock Min Max",udfStockMinMax($"donut_name"))
    df2.show()

    val firstRow=donutsDF.first()
    println(s"first row = $firstRow")

    val firstRowColumn1=donutsDF.first().get(0)
    println(s"first column data =$firstRowColumn1")

    val firstRowColumnPrice=donutsDF.first().getAs[Double]("donut_price")
    println(s"first row column price = $firstRowColumnPrice")

    val donuts2 = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
    val df = spark.createDataFrame(donuts2).toDF("Donut Name", "Price", "Purchase Date")

    df.withColumn("price formatted",format_number($"Price",2))
      .withColumn("Name formatted",format_string("awesome %s",$"Donut Name"))
      .withColumn("Name Uppercase",upper($"Donut Name"))
      .withColumn("Name lowercase",lower($"Donut Name"))
      .withColumn("Date Formatted",date_format($"Purchase Date","yyyyMMdd"))
      .withColumn("Day",dayofmonth($"Purchase Date"))
      .withColumn("Month",month($"Purchase Date"))
      .withColumn("Year",year($"Purchase Date"))
      .show()

    df.withColumn("Hash",hash($"Donut Name"))
      .withColumn("md5",md5($"Donut Name"))
      .withColumn("sha1",sha1($"Donut Name"))
      .withColumn("sha2",sha2($"Donut Name",256))
      .show()

    df.withColumn("Contain plain",instr($"Donut Name","donut"))
      .withColumn("Length",length($"Donut Name"))
      .withColumn("Trim",trim($"Donut Name"))
      .withColumn("LTrim",ltrim($"Donut Name"))
      .withColumn("RTrim",rtrim($"Donut Name"))
      .withColumn("Reverse",reverse($"Donut Name"))
      .withColumn("SubString",substring($"Donut Name",0,5))
      .withColumn("IsNull",isnull($"Donut Name"))
      .withColumn("concate1",concat_ws(" - ",$"Donut Name",$"Price"))
      .withColumn("Concat",concat($"Donut Name",$"Price"))
      .withColumn("InitCap",initcap($"Donut Name"))
      .show()

    val donuts3 = Seq(("plain donut", 1.50), (null.asInstanceOf[String], 2.0), ("glazed donut", 2.50))

    val dfWithNull=spark.createDataFrame(donuts3).toDF("Donut Name","Donut Price")
    dfWithNull.show()

    dfWithNull.na.drop().show()
  }
