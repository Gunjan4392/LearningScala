package com.gkd.spark.flattenUtility

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io.PrintWriter
import org.apache.commons.io.FilenameUtils._
import org.apache.log4j._

object metaGeneration {
  val log: Logger = LogManager.getLogger(flattenJSON.getClass)
  
  def metaFile(spark: SparkSession, fileStatus: Array[FileStatus]) {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    for (x <- fileStatus) {
      val fileName = getBaseName(x.getPath.getName)
      val filePath = getPath(x.getPath.toString)
      val metaFile = new Path(filePath +"/" + fileName + ".meta")
      val output = fs.create(metaFile)
      val writer = new PrintWriter(output)
      val fileSize = x.getLen
      val numRecords = spark.sparkContext.textFile(x.getPath.toString).count

      writer.write("s3://marsh-flatten-bucket/" + '\t' + 0 + '\t' + numRecords + '\t' + fileSize)
      log.info("Meta File created and moved to HDFS Path : " + metaFile)
      writer.close
    }
  }
}
