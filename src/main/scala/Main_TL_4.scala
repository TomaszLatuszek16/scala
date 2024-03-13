package tomasz.spark_project

import org.apache.spark.sql.SparkSession


object Main_TL_4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------

    val rddFromFile = spark.sparkContext.textFile("data_tom/AAPL.csv")

    rddFromFile.foreach(f=>{
      println(f)
    })


// --------------------------------------------------------------------------------------------------------


  }
}
