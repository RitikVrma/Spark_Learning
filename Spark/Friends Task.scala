// Databricks notebook source
// /FileStore/tables/FriendsData.csv
//avg number of friend for each age

// COMMAND ----------

val friendData = sc.textFile("/FileStore/tables/FriendsData.csv")
friendData.take(5)

// COMMAND ----------

val removeHeader = friendData.filter(x=> !x.contains("Id")) //removing header
removeHeader.take(3)

// COMMAND ----------

val ageRDD = removeHeader.map(x => (x.split(",")(2).toInt, (1,x.split(",")(3).toFloat)))
ageRDD.collect()

// COMMAND ----------

ageRDD.map(x=>x._1).take(10)

// COMMAND ----------

ageRDD.map(x=>x._2).take(10)

// COMMAND ----------

val reducedRDD = ageRDD.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
reducedRDD.take(10)

// COMMAND ----------

val avgRDD = reducedRDD.mapValues(data => data._2/ data._1)
avgRDD.collect()

// COMMAND ----------

// DBTITLE 1,Cal Average
for ((age,avgFriend) <- avgRDD.collect())
  println (age + ":" + avgFriend)

// COMMAND ----------

// DBTITLE 1,Task 2
val friendrdd=sc.textFile("/FileStore/tables/FriendsData.csv")
friendrdd.take(5)

// COMMAND ----------

val friendrmHead = friendrdd.filter(line => !line.contains("Age"))

// COMMAND ----------

friendrmHead.take(5)

// COMMAND ----------

val splitfriend=friendrmHead.map(x=> (x.split(",")(2).toInt,x.split(",")(3).toInt))

// COMMAND ----------

splitfriend.take(5)

// COMMAND ----------

val maxFriend=splitfriend.reduceByKey(math.max(_,_))
maxFriend.collect()

// COMMAND ----------

val eachagemax = maxFriend.sortByKey()
eachagemax.collect()

// COMMAND ----------


