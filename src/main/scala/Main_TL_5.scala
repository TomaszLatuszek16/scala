package tomasz.spark_project

import org.apache.spark.sql.SparkSession


object Main_TL_5 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------

    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.show()

    df.groupBy("department", "state").sum("salary").show()
    df.groupBy("department", "state").sum("salary","bonus").show()

    import org.apache.spark.sql.functions._

    df.groupBy("state").agg(sum("salary"),count("bonus"))

    df.groupBy("state")
      .agg(sum("salary").as("salarySUM"),
        count("bonus").as("bonusCOUNT"))
      .where(col("salarySUM")>1000)
      .show()



// --------------------------------------------------------------------------------------------------------


  }
}
