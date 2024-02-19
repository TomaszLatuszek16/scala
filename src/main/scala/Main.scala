
import org.apache.hadoop.io.nativeio.NativeIO.Windows
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark-scala-project")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()

    val df = spark.read
      .option("header", value=true)
      .option("inferSchema",value=true)
      .csv("data_tom/AAPL.csv")

// --------------------------------------------------------------------------------------------------------
    df.show()
    df.printSchema()
    df.select("open","Close").show() //ctrl + b
    val c=col("open")

    val column = df("Open")
    val newColumn = (column +2).as("OpenColumnsIncreasedBy2")
    val columnString = column.cast(StringType)
//----------------------------------------------------------------------------------
    val newColumn1=lit(2.0)
    val newColumn2=concat(newColumn1,lit("hello Word"))
//----------------------------------------------------------------------------------
    val Date_using_expr = expr("cast(current_timestamp() as string) as Date_using_SQL_expr")
    val Date_using_function = current_timestamp().cast(StringType).as("Date_using_function")
//----------------------------------------------------------------------------------

    df.select(Date_using_expr,Date_using_function).show()
    df.selectExpr("cast(Date as string)","Open+2.0", "current_timestamp()").show()
//----------------------------------------------------------------------------------
    //spark.sql to use is not recommended
    df.createTempView("df")
    spark.sql("select * FROM df").show()

    df.select(columnString, newColumn,newColumn2).show()

    df.select(column,columnString, newColumn)
      .filter(newColumn>3)
      .filter(newColumn>columnString)
      .show(truncate=false)

//----------------------------------------------------------------------------------
    //rename columns
    df.withColumnRenamed("Open","open").show()

    //better way
    val rename_columns = List(
     col("Open").as("open"), col("Close").as("close")
    )

    //we use special trick that flattens that list
    df.select(rename_columns: _*).show()

    //nicer way below
    df.select(df.columns.map(c => col(c).as(c.toLowerCase())):_*).show()
//----------------------------------------------------------------------------------
  //creating new attribute, it could be written as 2 new variables
    val df_better_names = df.select(rename_columns: _*)
    .withColumn("diff",col("open") - col("close"))
      .filter(col("close") > col("open")*1.1)

    df_better_names.show()
//------------------------------------------------------------------------------------
    //11 - GroupBy, sort, Aggregation
    //ALT + Enter - to get libraries we need
     //df.groupBy.($"Open") is easier then df.groupBy(col("Open"))
    import spark.implicits._

    df.groupBy(year($"Date").as("year"))
      .agg(max($"Close").as("maxClose"),avg($"Close").as("avgClose"))
      .sort($"maxClose".desc)
      .show()

    df.groupBy(year($"Date").as("year"))
      .max("Close","high")
      .show()
//----------------------------------------------------------------------------------
    // TO DO
  //12 - windows functions
val Window = Window.partitionBy




//----------------------------------------------------------------------------------
    //13 - TO DO
    df.groupBy(year($"Date").as("year"))
      .max("Close","high")
      .explain(extended=true)






  }
}
