package com.gkd.spark.flattenUtility

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs._
import com.gkd.spark.flattenUtility.flattenJSON._
import com.gkd.spark.flattenUtility.metaGeneration._
import com.gkd.spark.flattenUtility.hdfsToS3._

object flattenMain {

  private val conf: SparkConf = new SparkConf()
    .setAppName("flattenUtility")
    .set("spark.sql.parquet.compression.codec", "snappy")
  @transient val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val log: Logger = LogManager.getLogger(flattenMain.getClass)
  val snappyPattern = "snappy"
  val metaPattern = "meta"
  val s3Path = "s3://ma-test-onkar001/"

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    if (args.length < 2) {
      log.error("Usage: FlattenJSON <inputFilePath> <outputPath>")
      System.exit(1)
    }

    //Path Variables
    val inputPath = args(0)
    val outputPath = args(1)

    //Read JSON file
    val df_InputJSON = spark.read.json(inputPath)
    
    log.info("Read JSON : " + inputPath)

    //Flatten Nested JSON
    val flattenedDataFrame = flattenDataframe(df_InputJSON)
    log.info("JSON Flattened")

    val fileStatus = saveFlatJSON(spark, flattenedDataFrame, inputPath, outputPath)
    metaFile(spark, fileStatus)
    moveJSONtoS3(spark, outputPath, snappyPattern, s3Path)
    moveJSONtoS3(spark, outputPath, metaPattern, s3Path)
  }
}
