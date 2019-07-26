package com.gkd.spark.flattenUtility

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object hdfsToS3 {

  val log: Logger = LoggerFactory.getLogger(flattenJSON.getClass)

  def moveJSONtoS3(spark: SparkSession, source: String, srcPattern: String, target: String) = {
    log.info("Moving JSON files from HDFS to S3 !!")

    import scala.sys.process._

    log.info("stopping spark context explicitly")
    if (!spark.sparkContext.isStopped)
      spark.stop()
    log.info("spark context stopped explicitly")

    try {

      val S3_DIST_CP = "s3-dist-cp"

      val cmdBuilder = StringBuilder.newBuilder

      cmdBuilder.append(S3_DIST_CP)
        .append(" --src ")
        .append(source)
        .append(" --dest ")
        .append(target)
        .append(" --srcPattern .*" + srcPattern)

      val cmd = cmdBuilder.toString
      //-Dmapred.child.java.opts=-Xmx1024m -Dmapreduce.job.reduces=2

      log.info("full hdfs to s3 command : [{}]", cmd)

      // command execution
      val exitCode = (stringToProcess(cmd)).!

      log.info("s3_dist_cp command exit code: {} and s3 copy got " + (if (exitCode == 0) "SUCCEEDED" else "FAILED"), exitCode)

      if (exitCode == 0) {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val sourcePath = new Path( new Path(source), srcPattern)
        if (fs.exists(sourcePath)) {
          fs.delete(sourcePath, true)
          log.info("Deleted the files from hdfs " + sourcePath)
        }
      }

    } catch {
      case ex: Exception =>
        log.error(
          "there was an exception while copying JSON file to s3 bucket. {} {}",
          "", ex.getMessage, ex)
      //   throw new IngestionException("s3 dist cp command failure", null, Some(StatusEnum.S3_DIST_CP_CMD_FAILED))
    }
  }

}
