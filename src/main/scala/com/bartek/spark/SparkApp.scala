package com.bartek.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkApp {
  private val LOGGER: Logger = LoggerFactory.getLogger(getClass.getName.split('$')(0))

  def main(args: Array[String]): Unit = {
    val argsMap = collection.mutable.Map() ++ args.map(arg => arg.split("=")(0) -> arg.split("=")(1)).toMap
    val appName = SparkApp.getClass.getSimpleName.split('$')(0)
    val sparkConf = new SparkConf().setAppName(appName)
    if (isLocal) {
      sparkConf.setMaster("local[*]")
      val projectId = System.getenv("GCP_PROJECT") // replace with your project id
      argsMap.put("projectId", projectId)
      argsMap.put("bucket", s"$projectId-bartek-dataproc")
      argsMap.put("sourceTable", "bartek_person.bartek_person_table")
      argsMap.put("targetTable", "bartek_person.bartek_person_spark")
    }
    LOGGER.info("argsMap={}", argsMap)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    var dataDF: DataFrame = spark.read.format("avro")
      .load(s"gs://${argsMap("bucket")}/myRecord.snappy.avro")

    val refDF = spark.read.format("bigquery")
      .option("viewsEnabled", "true")
      .option("materializationDataset", "bartek_person")
      .load(s"SELECT distinct name, UPPER(name) as uname FROM ${argsMap.get("sourceTable")}")

    val refMap = refDF.collect.map(row => row(0) -> row(1)).toMap.asInstanceOf[Map[String, String]]
    val refBroadcast = spark.sparkContext.broadcast(refMap)

    val getCountry = (name: String) => {
      refBroadcast.value.getOrElse(name, "UNKNOWN")
    }
    val getCountryUDF = udf(getCountry)

    dataDF = dataDF.withColumn("uname", getCountryUDF(col("name")))
    dataDF = dataDF.map((p: Row) => {
      val name = p.getAs[String]("name")
      val body = p.getAs[Array[Byte]]("body")
      val uname = p.getAs[String]("uname")
      LOGGER.info("processing {}", (name, new String(body), uname))
      Thread.sleep(100)
      (name, body, uname)
    }).toDF(dataDF.columns: _*)

    dataDF.write.format("bigquery")
      .option("writeMethod", "indirect")
      .option("temporaryGcsBucket", argsMap("bucket"))
      .mode(SaveMode.Append)
      .save(argsMap("targetTable"))

    spark.stop()
  }

  private def isLocal: Boolean = {
    val osName = System.getProperty("os.name").toLowerCase
    osName.contains("mac") || osName.contains("windows")
  }

}
