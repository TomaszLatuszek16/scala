package tomasz.spark_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit


object Main_TL_2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

//    val df = spark.read
//      .option("header", value=true)
//      .option("inferSchema",value=true)
//      .csv("data_tom/AAPL.csv")

// --------------------------------------------------------------------------------------------------------


    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
      Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
      Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType
        ))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

    val df4 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    df4.printSchema()
    df4.show()

    df4.where(df4("state")==="OH").show()

    df4.withColumn("state_new",lit("test"))
    df4.withColumn("age",lit(1)).show()
    val df5=df4.withColumn("age",lit(1))
//    val df5 = df4.withColumn("age_new",df4("age")*100)

    df5.show()
    df5.withColumnRenamed("age","age_age").show()
    df5.show()


//    df5





  }
}
