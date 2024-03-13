package tomasz.spark_project


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Main_TL_1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------



// --------------------------------------------------------------------------------------------------------
// Create data
    import spark.implicits._
    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd = spark.sparkContext.parallelize(data)

    val df = rdd.toDF(columns:_*)
    val df1 = spark.createDataFrame(rdd).toDF(columns:_*)

    df1.printSchema()
    df1.show()

//    -------------------

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    val schema = StructType(
      Array(
        StructField("language",StringType,true),
        StructField("users",StringType,true)
      )
    )

    //Here, attributes._1 and attributes._2 represent the first and second components of each element in the original RDD
    val rawRDD=rdd.map(attribute=>Row(attribute._1,attribute._2))
    val df3 = spark.createDataFrame(rawRDD,schema)
    df3.show()


  }
}
