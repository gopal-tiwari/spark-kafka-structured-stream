package org.connected.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object StreamReader
{

  def getKafkaStreamDataFrame(spark: SparkSession) =
  {
    val streamingDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "demotopic")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", 100)

    streamingDF.load()
  }

  def streamColumnMapping(kafkaStreamDataframe: DataFrame) =
  {
    val kafkaDataSchema = StructType(List(
      StructField("dataTimestamp", StringType),
      StructField("data", StringType)
    ))

    val streamingDataWithColumn = kafkaStreamDataframe.select(
      col("timestamp").alias("kafka_timestamp"),
      col("partition"),
      col("offset"),
      col("key"),
      from_json(col("value").cast("String"), kafkaDataSchema).getField("data").alias("event_data"),
      from_json(col("value").cast("String"), kafkaDataSchema).getField("dataTimestamp").cast("timestamp").alias("data_timestamp"),
      date_format(col("timestamp"), "yyyy-MM-dd").alias("process_date"),
      hour(col("timestamp")).alias("hour"),
      minute(col("timestamp")).alias("minute")
    )
      .withColumn("new_col", col("event_data"))
    streamingDataWithColumn
  }

  def kafkaHDFSSink(streamingDataFrame: DataFrame) =
  {
    streamingDataFrame
      .writeStream
      .format("parquet")
      .option("path", "/tmp/stream_data")
      .option("checkpointLocation", "/tmp/stream_checkpoint")
      .partitionBy("date", "hour", "minute")
  }

}
