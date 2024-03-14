package tomasz.spark_project

import org.apache.spark.sql.SparkSession

object Main_TL_8 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------

    val data = Seq(
      ("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")
    )

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    val countries = Seq("USA","China","Canada","Mexico")
    val pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF.show()

// --------------------------------------------------------------------------------------------------------


  }
}
