import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.csv("assets/CitiGroup2006_2008")

// NOTES: DataFrame is like an Excel spreadsheet, where the columns are the DataSets
df.head(5)

//org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 4 more fields]
// Array[org.apache.spark.sql.Row] = Array([Date,Open,High,Low,Close,Volume], [2006-01-03,490.0,493.8,481.1,492.9,1537660], [2006-01-04,488.6,491.0,483.5,483.8,1871020], [2006-01-05,484.4,487.8,484.0,486.2,1143160], [2006-01-06,488.8,489.0,482.0,486.2,1370250])
//NOTES:
// * _c0, _c1 -> default way of calling columns in Spark
// * Spark uses string as a type by default

