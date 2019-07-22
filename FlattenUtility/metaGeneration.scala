package com.impetus.spark.flattenUtility

import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io.PrintWriter
import org.apache.commons.io.FilenameUtils._

object metaGeneration {

  def metaFile(sc: SparkContext, fileStatus: Array[FileStatus]) {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    for (x <- fileStatus) {
      val fileName = getBaseName(x.getPath.getName)
      val metaFile = new Path("/tmp/meta/" + fileName + ".meta")
      val output = fs.create(metaFile)
      val writer = new PrintWriter(output)

      val fileSize = x.getLen
      val numRecords = sc.textFile(metaFile.toString).count()
      writer.write("arn:aws:s3:::impetus-flattenbucket" + '\t' + 0 + '\t' + numRecords + '\t' + fileSize)
      writer.close()
    }
  }
}
