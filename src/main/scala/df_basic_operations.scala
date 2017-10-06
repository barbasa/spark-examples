import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("assets/CitiGroup2006_2008")

import spark.implicits._

// Scala notation
//df.filter($"Close">480).show(2)
//
//// Spark SQL notation to do the same thing...
//df.filter("Close > 480").show(2)


//df.filter($"Close"<480 && $"High"<480).show(2)
//df.filter("Close < 480 AND High < 480").show(2)
//
//// Notice the triple = ...it is a bug of Spark 2.0.1
//df.filter($"High"===484.40).show()
//df.filter("High = 484.40").show()

// Maybe you don't want to display the result on screen, but you wanna store the result in an object
//val CH_low = df.filter("Close < 480 AND High < 480").collect()
//
//val count = df.filter("Close < 480 AND High < 480").count()

// CORRELATION...and other functions: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
df.select(corr("High","Low")).show()


