package tomasz.spark_project

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object Main_TL_4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()


// --------------------------------------------------------------------------------------------------------


    val structureData = Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )

    val structureSchema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("id",StringType)
      .add("location",StringType)
      .add("salary",IntegerType)

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),structureSchema)
    df2.printSchema()
    df2.show(false)

    import spark.implicits._
    val df3 = df2.map(row=>{
      val fullName = row.getString(0) +row.getString(1) +row.getString(2)
      (fullName, row.getString(3),row.getInt(5))
    })
    val df3Map =  df3.toDF("fullName","id","salary")

    df3Map.printSchema()
    df3Map.show(false)


// --------------------------------------------------------------------------------------------------------


  }
}
