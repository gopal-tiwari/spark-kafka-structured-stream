package org.connected.kafka

import org.apache.spark.sql.DataFrame

object StreamReader
{

  def kafkaHDFSSink(streamingDataFrame:DataFrame)={
    streamingDataFrame
      .writeStream
      .format("parquet")
      .option("path", "/tmp/stream_data")
      .option("checkpointLocation","/tmp/stream_checkpoint")
      .partitionBy("date", "hour", "minute")
  }

}
