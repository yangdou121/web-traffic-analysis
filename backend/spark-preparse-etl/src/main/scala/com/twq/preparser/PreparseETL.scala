package com.twq.preparser

import com.twq.prepaser.{PreParsedLog, WebLogPreParser}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object PreparseETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PreparseETL")
      .enableHiveSupport()
      .master("local")
      .config("spark.testing.memory", "512000000")
      .getOrCreate()


    val rawdataInputPath = spark.conf
      .get("spark.traffic.analisis.rawdata.input" ,
      "hdfs://master:9999/user/hadoop-twq/traffic-analysis/rawlog/20180615")
    //从Spark配置中获取分区数，如果配置不存在，则使用默认值2
    val numberPartitions = spark.conf.get("spark.traffic.analisis.rawdata.numberPartitions" ,
      "2").toInt

    val preParsedLogRDD = spark.sparkContext.textFile(rawdataInputPath)
      .flatMap(line => Option(WebLogPreParser.parse(line)))
    val preParsedLogDS = spark.createDataset(preParsedLogRDD)(Encoders.bean(classOf[PreParsedLog]))
   //重新分区并保存数据到HIVE表
    preParsedLogDS.coalesce(numberPartitions)  //合并分区数
      .write
      .mode(SaveMode.Append) //追加模式写入
      .partitionBy("year","month","day")//// 按年、月、日分区
      .saveAsTable("rawdata.web")//保存到HIVE表
    spark.stop();


  }

}
