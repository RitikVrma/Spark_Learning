// Databricks notebook source
//  /FileStore/tables/airports-1.text

val airData = sc.textFile("/FileStore/tables/airports-1.text")


// COMMAND ----------

val airrdd = airData.map(x => (x.split(",")(1), x.split(",")(11).toLowerCase))
airrdd.take(3)

// COMMAND ----------

// DBTITLE 1,latitude >40 Or Country = Island
val localData = airData.filter(line => { (line.split(",")(6) > "\"40\"") && (line.split(",")(3).contains("Island"))})
localData.collect()

// COMMAND ----------

localData.saveAsTextFile("AirportTask.csv")

// COMMAND ----------

// DBTITLE 1,Task 2
val occurence = airData.filter(line => {
  (line.split(",")(11).contains("Pacific/Port_Moresby"))&& ((line.split(",")(8).toInt)%2 ==0)
})
occurence.count()
