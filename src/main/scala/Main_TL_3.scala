package tomasz.spark_project

import org.apache.spark.sql.SparkSession


object Main_TL_3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------

    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df6 = simpleData.toDF("employee_name", "department", "salary")
    df6.show()

    val distinct_df6 =df6.distinct()
    println("Distinct count: "+distinct_df6.count())
    distinct_df6.show(false)

// --------------------------------------------------------------------------------------------------------


  }
}
