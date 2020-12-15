// Databricks notebook source
//  /FileStore/tables/numberData.csv
val numbers = sc.textFile("/FileStore/tables/numberData.csv")
val header = numbers.first
val headerless = numbers.filter(line=>line!=header)

val intData = headerless.map(line=>line.toInt)

def prime(a:Int): Boolean={
  var r = true
  var x = 0
  if(a == 0|| a==1)
  {
    return false
  }
  else{
    for(x <- 2 until a){
      if(a%x ==0){
        r = false
      }
    }
  }
  return r
}

intData.collect()

val primeFilter = intData.filter(line=>prime(line))
primeFilter.collect()
val sumData = primeFilter.sum()
println("Total sum "+sumData)
primeFilter.count()

// COMMAND ----------

val listData = List("Tushar 2020","Ritik 2000","Rohit 1900")

// COMMAND ----------

val newRDD = sc.parallelize(listData) 

// COMMAND ----------

val rddnew = newRDD.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))
rddnew.take(3)

// COMMAND ----------

rddnew.mapValues(x => x+10).take(3)
