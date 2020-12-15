// Databricks notebook source
// /FileStore/tables/Property_data.csv

// COMMAND ----------

val data = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removeHeader = data.filter(line => !line.contains("Price"))
removeHeader.take(10)

// COMMAND ----------

val roomRDD = removeHeader.map(x=>(x.split(",")(3).toInt, (1,x.split(",")(2).toDouble)))

roomRDD.collect()

// COMMAND ----------

val reducedRDD = roomRDD.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
reducedRDD.take(10)

// COMMAND ----------

val avgRDD = reducedRDD.mapValues(data => data._2 / data._1)
avgRDD.collect()
