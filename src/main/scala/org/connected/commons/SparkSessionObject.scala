package org.connected.commons

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSessionObject
{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("hive").setLevel(Level.OFF)
  Logger.getLogger("hadoop").setLevel(Level.OFF)
  Logger.getLogger("hdfs").setLevel(Level.OFF)
  Logger.getRootLogger().setLevel(Level.OFF)

  val spark: SparkSession = SparkSession.builder
    .appName("Spark-Kafka")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()



}
