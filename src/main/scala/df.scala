import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("assets/CitiGroup2006_2008")

// NOTES: DataFrame is like an Excel spreadsheet, where the columns are the DataSets
//df.head(5)

//org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 4 more fields]
// Array[org.apache.spark.sql.Row] = Array([Date,Open,High,Low,Close,Volume], [2006-01-03,490.0,493.8,481.1,492.9,1537660], [2006-01-04,488.6,491.0,483.5,483.8,1871020], [2006-01-05,484.4,487.8,484.0,486.2,1143160], [2006-01-06,488.8,489.0,482.0,486.2,1370250])
//NOTES:
// * _c0, _c1 -> default way of calling columns in Spark
// * Spark uses string as a type by default

df.columns
for (row <- df.head(5)) {
  println(row)
}
// NOTES: Stats out of the box!
//df.describe().show()

// NOTES: After adding the inferSchema option the data types are correct
// df: org.apache.spark.sql.DataFrame = [Date: timestamp, Open: double ... 4 more fields]


// NOTES: Select the columns you are interested in
df.select($"Volume",$"Date").show(2)

// NOTES: Add a new column. df("High") takes the column object
val df2 = df.withColumn("HighPlusLow", df("High") + df("Low"))

df2.printSchema()

df2.select(df2("HighPlusLow").as("HPL"), df2("Close")).show(2)



