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


// pieknem Scali jest mozliwosc pisania szybkie klasy i jej typow danych!!
case class City(rank: Long, city: String, state: String, code: String, population: Long, price: Double)
val df1 = Seq(new City(295, "South Bend", "Indiana", "IN", 101190, 112.9)).toDF

display(df1)



val df2 = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")



// Returns a DataFrame that combines the rows of df1 and df2
val df = df1.union(df2)
// Returns a DataFrame that combines the rows of df1 and df2
val df = df1.union(df2)
val select_df = df.select("City", "State")
val subset_df = df.filter(df("rank") < 11).select("City")

//Databricks uses the Delta Lake format for all tables by default. To save your DataFrame, you must have CREATE table privileges on the catalog and schema. The following example saves the contents of the DataFrame to a table named us_cities:
df.write.saveAsTable("us_cities")



//The selectExpr() method allows you to specify each column as a SQL query, such as in the following example:
display(df.selectExpr("`rank`", "upper(city) as big_name"))
import org.apache.spark.sql.functions.expr

//You can import org.apache.spark.sql.functions.expr to use SQL syntax anywhere a column would be specified, as in the following example:
display(df.select($"rank", expr("lower(city) as little_name")))

val table_name = "us_cities"
val query_df = spark.sql(s"SELECT * FROM $table_name")


// Create data
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

// Spark Create DataFrame from RDD
// RDD is schema-less
val rdd = spark.sparkContext.parallelize(data)

//In PySpark, parallelize(data) is used to create an RDD (Resilient Distributed Dataset) from a local collection or iterable data. This function distributes the data across the Spark cluster, allowing parallel processing of the elements within the RDD. It is a fundamental operation for leveraging the distributed computing capabilities of Apache Spark. (see above picture)

// Create DataFrame from RDD
// Use toDF() on RDD object to create a DataFrame in Spark. By default, it creates column names as “_1” and “_2” as we have two columns for each row.
import spark.implicits._
val dfFromRDD1 = rdd.toDF()


dfFromRDD1.printSchema()
dfFromRDD1.show()





//Since RDD is schema-less without column names and data type, converting from RDD to DataFrame gives you default column names as _1, _2 and so on and data type as String. Use DataFrame printSchema() to print the schema to console.

//toDF() has another signature to assign a column name, this takes a variable number of arguments for column names as shown below.

val dfFromRDD1 = rdd.toDF("language","users_count")
dfFromRDD1.show()
dfFromRDD1.printSchema()

val columns = Seq("language","users_count")

// val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
// val columns = Seq("language","users_count")

// Using createDataFrame()
val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)

//Here, toDF(columns: _*): assigns column names to the DataFrame using the provided columns list or sequence. The _* is a syntax to pass a variable number of arguments. It facilitates converting the elements in columns into separate arguments for the toDF method.


//createDataFrame() has another signature that takes the RDD[Row] type and schema for column names as arguments. To use this first, we need to convert our “rdd” object from RDD[T] to RDD[Row] and define a schema using StructType & StructField.

// Additional Imports
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

// Create StructType Schema
val schema = StructType( Array(
  StructField("language", StringType,true),
  StructField("users", StringType,true)
))

// Use map() transformation to get Row type
val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2)) //anonymous function
val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)

//Here, attributes._1 and attributes._2 represent the first and second components of each element in the original RDD. The transformation maps each element of rdd to a Row object with two fields, essentially converting a pair of attributes into a structured row


// 2. Create Spark DataFrame from List and Seq Collection
//In this section, we will see several approaches of how to create Spark DataFrame from collection Seq[T] or List[T]. These examples would be similar to what we have seen in the above section with RDD, but we use “data” object instead of “rdd” object.

// collection in scala: (Seq, List)

//2.1 Using toDF() on List or Seq collection
//The toDF() on collection (Seq, List) object creates a Spark DataFrame. Make sure importing import spark.implicits._ to use toDF(). In Apache Spark using Scala, import spark.implicits._ enables implicit conversions to Spark’s Dataset and DataFrame API.

// Import implicits
import spark.implicits._

// val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
// val columns = Seq("language","users_count")

// Create DF from data object
val dfFromData1 = data.toDF()

//Here, val dfFromData1 = data.toDF() creates a DataFrame (dfFromData1) from a local collection or Seq data. The toDF() method converts the collection into a DataFrame, automatically assigning default column names. The import statement is necessary for the implicit conversion to work.


//2.2 Using createDataFrame() from SparkSession
//Calling createDataFrame() from SparkSession is another way to create a Spark DataFrame, and it takes collection object (Seq or List) as an argument. and chain with toDF() to specify column names.

// val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
// val columns = Seq("language","users_count")


// From Data (USING createDataFrame)
var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)

//Here, toDF(columns: _*): assigns column names to the DataFrame using the provided columns list or sequence. The _* is a syntax to pass a variable number of arguments. It facilitates converting the elements in columns into separate arguments for the toDF method.


//2.3 Using createDataFrame() with the Row type
//createDataFrame() has another signature in Spark that takes the util.List of Row type and schema for column names as arguments. To use this first, we need to import scala.collection.JavaConversions._

//In Java terms, Scala's Seq would be Java's List

// Import
import scala.collection.JavaConversions._

// From Data (USING createDataFrame and Adding schema using StructType)
val rowData= Seq(Row("Java", "20000"),
  Row("Python", "100000"),
  Row("Scala", "3000"))

var dfFromData3 = spark.createDataFrame(rowData)

//JAKOS TO NIE DZIALA
//JAKOS TO NIE DZIALA
//JAKOS TO NIE DZIALA


// Creating from text (TXT) file
val df2 = spark.read
  .text("/resources/file.txt")

val df2 = spark.read
  .json("/resources/file.json")

// From Mysql table
val df_mysql = spark.read.format(“jdbc”)
.option(“url”, “jdbc:mysql://localhost:port/db”)
.option(“driver”, “com.mysql.jdbc.Driver”)
.option(“dbtable”, “tablename”)
.option(“user”, “user”)
.option(“password”, “password”)
.load()




import spark.implicits._

//The toDF() method can be called on a sequence object to create a DataFrame.

val someDFF = Seq(
  (8, "bat"),
  (64, "mouse"),
  (-27, "horse")
).toDF("number", "word")

//toDF() is limited because the column type and nullable flag cannot be customized. In this example, the number column is not nullable and the word column is nullable.

//toDF() is suitable for local testing, but production grade code that’s checked into master should use a better solution.

//The createDataFrame() method addresses the limitations of the toDF() method and allows for full schema customization and good Scala coding practices.
val someData = Seq(
  Row(8, "bat"),
  Row(64, "mouse"),
  Row(-27, "horse")
)

val someSchema = List(
  StructField("number", IntegerType, true),
  StructField("word", StringType, true)
)

val someDFFF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType(someSchema)
)

someDFFF.show()
//createDataFrame() provides the functionality we need, but the syntax is verbose. Our test files will become cluttered and difficult to read if createDataFrame() is used frequently.

//createDF() is defined in spark-daria and allows for the following terse syntax.

//createDF() creates readable code like toDF() and allows for full schema customization like createDataFrame(). It’s the best of both worlds.

//We’ll demonstrate why the createDF() method defined in spark-daria is better than the toDF() and createDataFrame() methods from the Spark source code.

//[Reason why it does not work]
//[StackOverFlow comment] CreateDF() is not SparkSession method. It is spark-daria method. You need to install dependancy and import the spark-daria library them you should be able to use it. Below article for your reference.

val someDF = spark.createDF(
  List(
    (8, "bat"),
    (64, "mouse"),
    (-27, "horse")
  ), List(
    ("number", IntegerType, true),
    ("word", StringType, true)
  )
)

//createDF() creates readable code like toDF() and allows for full schema customization like createDataFrame(). It’s the best of both worlds.


// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

val df = spark.read.json("examples/src/main/resources/people.json")

// Displays the content of the DataFrame to stdout
df.show()
// Select only the "name" column
df.select("name").show()
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
df.select($"name", $"age" + 1).show()
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
df.filter($"age" > 21).show()
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
df.groupBy("age").count().show()
// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+

import org.apache.spark.sql.types._

// Create an RDD
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")

// The schema is encoded in a string
val schemaString = "name age"

// Generate the schema based on the string of schema
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))

val schema = StructType(fields)

// Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)

// Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL can be run over a temporary view created using DataFrames
val results = spark.sql("SELECT name FROM people")

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
results.map(attributes => "Name: " + attributes(0)).show()
// +-------------+
// |        value|
// +-------------+
// |Name: Michael|
// |   Name: Andy|
// | Name: Justin|
// +-------------+


val Products = Seq(
  (1, "shirt",2,800),
  (2, "Jeans",2,300),
  (3, "Watch",4,200),
  (4, "Toys",8,150),(5, "cooldrinks",10,540))
  .toDF("Product_ID", "Product_Name","Tax","Cost")

Products.show()



val Products = Seq(
  (1, "shirt",2,800),
  (2, "Jeans",2,300),
  (3, "Watch",4,200),
  (4, "Toys",8,150),
  (5, "cooldrinks",10,540)
)

val rdd = spark.sparkContext.parallelize(Products)
rdd.toDF("Product_ID","Product_Name","Tax","Cost").show()

val columns = Seq("Product_ID","Product_Name","Tax","Cost")
val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
dfFromRDD2.show()


import org.apache.spark.sql.types.{StringType,IntegerType, StructField, StructType}
import org.apache.spark.sql.Row

val Products = Seq(
  (1, "shirt",2,800),
  (2, "Jeans",2,300),
  (3, "Watch",4,200),
  (4, "Toys",8,150),
  (5, "cooldrinks",10,540)
)

val rdd = spark.sparkContext.parallelize(Products)
rdd.toDF("Product_ID","Product_Name","Tax","Cost").show()

val schema = StructType( Array(
  StructField("Product_ID", IntegerType,true),
  StructField("Product_Name", StringType,true),
  StructField("Tax", IntegerType,true),
  StructField("Cost", IntegerType,true))
)

val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2,attributes._3,attributes._4))

val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)

dfFromRDD3.show()


//Using createDataFrame() from SparkSession
val data = Seq((1, "shirt",2,800)
  (2, "Jeans",2,300),
  (3, "Watch",4,200),
  (4, "Toys",8,150),
  (5, "cooldrinks",10,540))

val columns = Seq("productid","productname","tax","cose")
var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
dfFromData2.show()



//Using createDataFrame() with the Row type.
//Row is a generic row object with an ordered collection of fields that can be accessed by an ordinal / an index (aka generic access by ordinal), a name (aka native primitive access) or using Scala's pattern matching.
import scala.collection.JavaConversions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, IntegerType}
import org.apache.spark.sql.Row

val rowData= Seq(Row(1,"shirt",2,800),
  Row(2,"Jeans", 2,300),
  Row(3,"Watch", 4,400),
  Row(4,"Toys", 8,200),
  Row(5,"cooldrinks", 5,540)
)
val schema = StructType( Array(
  StructField("productid", IntegerType,true),
  StructField("productname", StringType,true),
  StructField("tax", IntegerType,true),
  StructField("cost", IntegerType,true)
))
var dfFromData = spark.createDataFrame(rowData,schema)
dfFromData.show()



//Create Spark DataFrame from CSV (If File don’t have Schema).
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}

val dataschema = new StructType()
  .add(StructField("id", IntegerType, true))
  .add(StructField("ProductName", StringType, true))
  .add(StructField("Tax", IntegerType, true))
  .add(StructField("Cost",IntegerType, true))

val df = spark.read.format("csv")
  .option("delimiter", ",")
  .schema(dataschema)
  .load("File1.csv")

df.show()


//Create Spark DataFrame from CSV (If File have Schema).
val df = spark.read.format("csv")
  .option("delimiter", ",")
  .option("header","true")
  .load("File2.csv")

df.show()

//Creating Spark DataFrame from RDBMS Database.
val df_from_mysql = spark.read.format(“jdbc”)
.option(“url”, “jdbc:mysql://localhost:port/db”)
.option(“driver”, “com.mysql.jdbc.Driver”)
.option(“dbtable”, “tablename”)
.option(“user”, “user”)
.option(“password”, “password”)
.load()


//Adding and renaming a Column in SparkDataframe.
df.withColumn("Country",lit("India"))
df.withColumn("Country",lit("India")).show()
df.drop("cost").show()




//Case Statement In Dataframe:
val df2 = df.withColumn("category", when(col("productname") === "shirt","Cloth")
  .when(col("productname") === "Jeans","Cloth")
  .when(col("productname") === "Toys","Toys")
  .when(col("productname") === "cooldrinks","Drink")
  .otherwise("Unknown")).show()



//Filter Condition In Dataframe:
val df2 = df.withColumn("category", when(col("productname") === "shirt","Cloth")
  .when(col("productname") === "Jeans","Cloth")
  .when(col("productname") === "Toys","Toys")
  .when(col("productname") === "cooldrinks","Drink")
  .otherwise("Unknown")).filter(df("productname") === "shirt").show()



// Joins In Dataframe:
val df  = spark.read.format("csv").option("delimiter", ",").option("header","true").load("product.csv")
val df2 = spark.read.format("csv").option("delimiter", ",").option("header","true").load("category.csv")
df2.show()

val df3=df.join(df2,df("productid") ===  df2("id"),"inner")
df3.show()



val Products = Seq(
  (1, "hairoil",2,800),
  (2, "shampoo",2,300),
  (3, "soap",4,200),
  (4, "biscuit",8,150),
  (5, "cooldrinks",10,540)
).toDF("Product_ID", "Product_Name","Tax","Cost")

val Customers = Seq(("bharat", "tuticorn",2,2),
  ("Raj", "Chennai",3,3),
  ("ajay", "Bangalore",4,2),
  ("aswin", "Chennai",5,6),
  ("Gautham", "mumbai",1,7),
  ("bharat", "tuticorn",4,3),
  ("Raj", "Chennai",5,1),
  ("ajay", "Bangalore",2,1),
  ("aswin", "Chennai",3,6),
  ("Gautham", "mumbai",3,1)).toDF("Customer_Name","City","Product","Qty")

Products.show()
Customers.show()

val custInChennai =Customers
  .join(Products, $"Product" === $"Product_ID", "outer")
  .select("Customer_Name", "City", "Product_ID","Qty","Product_Name","Tax","Cost")
  .filter($"City"==="Chennai")
  .withColumn("TotalPrice",($"Tax".cast("Int")*$"Qty".cast("Int")*$"Cost".cast("Int")))
  .show()

val customerClassification =Customers
  .join(Products, $"Product" === $"Product_ID", "outer")
  .select("Customer_Name", "City", "Product_ID","Qty","Product_Name","Tax","Cost")
  .withColumn("TotalPrice",($"Tax".cast("Int")*$"Qty".cast("Int")*$"Cost".cast("Int")))
  .groupBy("Customer_Name")
  .agg(Map(
    "TotalPrice" -> "sum"))
  .withColumnRenamed("sum(TotalPrice)","TotalPrice")

customerClassification.show()

import org.apache.spark.sql.Column
val customerClassified = customerClassification
  .withColumn("TypeOfLiving",(when($"TotalPrice".cast("Int") <=1000,"Lower")
    .when($"TotalPrice".cast("Int") >1000,"Middle")
    .when($"TotalPrice".cast("Int") <=2000,"Upper Middle")
    .otherwise("UpperClass")))

customerClassified.show()

val customerClassification =Customers
  .join(Products, $"Product" === $"Product_ID", "outer")
  .select("Customer_Name", "City", "Product_ID","Qty","Product_Name","Tax","Cost")
  .withColumn("TotalPrice",($"Tax"
    .cast("Int")*$"Qty"
    .cast("Int")*$"Cost"
    .cast("Int")))
  .groupBy("Customer_Name")
  .agg(Map(
    "TotalPrice" -> "sum"))
  .withColumnRenamed("sum(TotalPrice)","TotalPrice")
  .sort($"TotalPrice".desc)
  .show(2)