// Databricks notebook source
// /FileStore/tables/nasa_july.tsv
// /FileStore/tables/nasa_august.tsv

val july = sc.textFile("/FileStore/tables/nasa_july.tsv")
val august = sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val julyHost = july.map(x => x.split("\t")(0))
val augustHost = august.map(x => x.split("\t")(0))

// COMMAND ----------

var intersectRDD = julyHost.intersection(augustHost)

// COMMAND ----------

def HeaderRemover (line: String):Boolean= !(line.startsWith("Host"))
intersectRDD.filter(x => HeaderRemover(x))count()
