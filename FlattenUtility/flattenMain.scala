package com.impetus.spark.flattenUtility

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Locale
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.internal._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.fs._
import com.impetus.spark.flattenUtility._

object flattenMain {

  //Set Log Level
  val logger = Logger.getLogger("org")
  val log: Logger = LogManager.getLogger(flattenMain.getClass)

  val conf = new SparkConf().setAppName("flattenUtility")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
  val spark = sqlContext.sparkSession
  val fs = FileSystem.get(sc.hadoopConfiguration)

  def main(args: Array[String]) {

    logger.setLevel(Level.ERROR)
    if (args.length < 1) {
      log.error("Usage: FlattenJSON <inputPath>")
      System.exit(1)
    }

    //Path Variables
    val inputPath = args(0)

    //Read JSON file
    val df_InputJSON = sqlContext.read.json(inputPath)
    log.info("Read JSON : "+inputPath)
    
    //Flatten Nested JSON
    val flattenedDataFrame = flattenJSON.flattenDataframe(sc,df_InputJSON)
    log.info("JSON Flattened")
    
    val numRecords = flattenedDataFrame.count
    val fileStatus = flattenJSON.saveFlatJSON(sc, spark, flattenedDataFrame, inputPath)
    metaGeneration.metaFile(sc,fileStatus)
    log.info("Meta Files Created")
    hdfsToS3.moveJSONtoS3(sqlContext,"/tmp/","arn:aws:s3:::impetus-flattenbucket")
    }
}
