package com.marsh.flatten

import java.time.format.DateTimeFormatter
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructType

import com.marsh.utils.Constants
import com.marsh.utils.Utils

/**
 * @author U1165077
 * Flatten JSON
 *
 */
class FlattenJSON(@transient val properties: Properties, @transient val spark: SparkSession) extends Serializable {
  @transient lazy val log = LogManager.getLogger(FlattenJSON.getClass)

  var date: String = java.time.LocalDate.now.format(DateTimeFormatter.BASIC_ISO_DATE)
  var tempOutDir: String = Constants.FLAT_TEMP_OUTPATH
  var inDir: String = Constants.BULK_OUTPATH
  var feed = ""
  var outDir = ""

  /**
   * Assign input arguments in variables
   */
  def init() = {
    this.feed = properties.getProperty("table")
    this.tempOutDir = this.tempOutDir + this.date + "/" + feed
    this.inDir = this.inDir + this.date + "/" + feed
    this.outDir = {
      if (!properties.getProperty("outpath").last.equals('/')) {
        properties.getProperty("outpath") + "/" + this.feed + "/"
      } else {
        properties.getProperty("outpath") + this.feed + "/"
      }
    }
  }

  /**
   * Flatten Flow
   *
   * Flatten i/p JSON, Rename and Persist flatten o/p JSON to HDFS, Generate Meta File
   */
  def process = {

    // read source data
    log.info("Start reading files from - " + inDir)

    val df = spark.read.json(inDir)

    // flatten nested json to flat json objects
    log.info("Start falttening the files from - " + inDir)

    val flattenDF = flatten(spark.sparkContext, df)

    // save falttened jsons to temp hdfs directory
    log.info("Persist flattened output to temp location - " + tempOutDir)
    flattenDF.write.option("compression", "snappy").mode("overwrite").json(tempOutDir)

    // rename part files generated in hdfs

    val prefix = properties.getProperty("table") + "_" + date + "_"
    val suffix = ".json.snappy"

    log.info("Renaming saved files in temp location with following prefix - [" + prefix + "]  and suffix - [" + suffix + "]")

    val pathList = Utils.getHdfsFilesList(tempOutDir)
    val renamedPathList = Utils.renameHdfsFiles(tempOutDir, pathList, prefix, suffix)

    log.info("Generating metafile for the generated parts for feed - " + feed)
    // generate metafile for the feed
    generateMetaFileSingleRecord(spark, renamedPathList)
  }

  /**
   *Flatten i/p DataFrame
   *
   * @param sc SparkContext
   * @param df i/p DataFrame
   * @return flattened DataFrame
   */
  def flatten(sc: SparkContext, df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length

    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name

      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_ != fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode: _*)
          return flatten(sc, explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replaceAll("[^a-zA-Z0-9_-]", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flatten(sc, explodedf)
        case _ =>
      }
    }
    df
  }

  /**
 * Create entry in the meta file for each Part file (3 entries will be created if 3 Part files are generated)
 * 
 * @param spark SparkSession
 * @param fileList Flattened and Renamed JSON FileList(persisted in HDFS)
 * @throws Exception
 */
def generateMetaFileMultiRecords(spark: SparkSession, fileList: ArrayBuffer[String]) = {

    if (fileList != null && !fileList.isEmpty) {
      val metaText: StringBuilder = StringBuilder.newBuilder
      try {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)

        fileList.foreach { x =>
          val s3Path = this.outDir + x
          val hdfsPath = this.tempOutDir + "/" + x

          val count = spark.read.json(hdfsPath).count

          val fileSize = fs.getFileStatus(new Path(hdfsPath)).getLen

          metaText.append(s3Path).append("\t") // add s3Path
          metaText.append(0).append("\t") // add seq number
          metaText.append(count).append("\t") // num records
          metaText.append(fileSize).append("\n") // file size
        }
        metaText.setLength(metaText.length() - 1) // remove last newline character

        log.info("Meta data generated - " + metaText.toString)

        // create metafile name
        val metaPath = this.tempOutDir + "/" + feed + "_" + date + ".meta"
        log.info("Persisting meta info to file -[" + metaPath + "]")
        // Write metaText to hdfs
        Utils.persistResponseToHDFS(metaText.toString, metaPath)

        log.info("Successfully persisted meta info to file -[" + metaPath + "]")

      } catch {
        case e: Exception => {
          log.error(e.getMessage)
          log.error(e.printStackTrace)
          throw e
        }
      }
    } else {
      throw new Exception("Cannot generate meta file for empty file list")
    }

  }

  /**
 * Create single entry in meta file for all Part files 
 * count and fileSize is sum of all individual part files
 * 
 * @param spark SparkSession
 * @param fileList Flattened and Renamed JSON FileList(persisted in HDFS)
 * @throws Exception
 */
  def generateMetaFileSingleRecord(spark: SparkSession, fileList: ArrayBuffer[String]) = {

    if (fileList != null && !fileList.isEmpty) {
      val metaText: StringBuilder = StringBuilder.newBuilder
      try {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)

        var s3Path = Array[String]()
        var hdfsPath = Array[String]()
        var count = 0: Long
        var fileSize = 0: Long

        fileList.foreach { x =>
          s3Path = s3Path :+ (this.outDir + x)
          hdfsPath = hdfsPath :+ (this.tempOutDir + "/" + x)
        }

        hdfsPath.foreach { x =>
          count = count + spark.read.json(x).count
          fileSize = fileSize + fs.getFileStatus(new Path(x)).getLen
        }

        metaText.append(s3Path.mkString(", ")).append("\t") // add s3Path
        metaText.append(0).append("\t") // add seq number
        metaText.append(count).append("\t") // num records
        metaText.append(fileSize).append("\n") // file size

        metaText.setLength(metaText.length() - 1) // remove last newline character

        log.info("Meta data generated - " + metaText.toString)

        // create metafile name
        val metaPath = this.tempOutDir + "/" + feed + "_" + date + ".meta"
        log.info("Persisting meta info to file -[" + metaPath + "]")
        // Write metaText to hdfs
        Utils.persistResponseToHDFS(metaText.toString, metaPath)

        log.info("Successfully persisted meta info to file -[" + metaPath + "]")

      } catch {
        case e: Exception => {
          log.error(e.getMessage)
          log.error(e.printStackTrace)
          throw e
        }
      }
    } else {
      throw new Exception("Cannot generate meta file for empty file list")
    }

  }

}

object FlattenJSON extends App {
  @transient lazy val log = LogManager.getLogger(FlattenJSON.getClass)
  /**
   * Application entry point
   *
   * @param outpath s3 raw bucket path
   * @param table Feed Name
   * @throws Exception
   */
  override def main(args: Array[String]) {

    log.setLevel(Level.INFO)

    log.info("FlattenJSON Initialization started")

    if (args.length < 2) {
      println("Job requires parameters in following order -")
      println("--outpath=<s3 raw bucket path>")
      println("--table=<feed name>")
      System.exit(-1)
    }
    //    System.setProperty("hadoop.home.dir", "C:\\Users\\U1165077\\Desktop\\windows_libs")

    log.setLevel(Level.INFO)
    log.info("FlattenJSON input parsing started")

    log.info("Recieved argutemnts : " + args.mkString("|"))

    var properties = new Properties

    properties.putAll(Utils.parseCmdLineArgs(args))

    log.info("FlattenJSON inpupt parsing completed")
    log.info("Populated user provided properties : " + properties.toString)

    // Create SparkSession

    log.info("Initializing Spark Session")
    var spark: SparkSession = null
    var applicationId = ""
    var flatten: FlattenJSON = null
    val output = Constants.FLAT_TEMP_OUTPATH + java.time.LocalDate.now.format(DateTimeFormatter.BASIC_ISO_DATE) + "/"
    val feed = properties.getProperty("table")

    try {
      spark = SparkSession.builder.appName("flattenjson").getOrCreate()
    } catch {
      case e: Exception => {
        val status = "Spark Session creation failed for feed : " + feed + " with error : " + e.getMessage
        Utils.handleException(feed, applicationId, -1, "FAILED", status, status, output)
        log.error(e.getMessage)
        log.error(e.printStackTrace)
        System.exit(-1)
      }
    }
    applicationId = spark.sparkContext.applicationId

    log.info("Spark Session initailized successfully with application id [" + applicationId + "]")

    log.info("Initailizing main class for flattening JSON " + FlattenJSON.getClass.getName)
    flatten = new FlattenJSON(properties, spark)

    flatten.init
    log.info("Initailization completed for main class " + FlattenJSON.getClass.getName)

    try {
      log.info("Flattening json files started")
      flatten.process
    } catch {
      case e: Exception => {
        val status = "Flattening JSON failed for feed : " + feed + " failed with error : " + e.getMessage
        Utils.handleException(feed, applicationId, -1, "FAILED", status, status, output)
        log.error(status)
        log.error(e.printStackTrace)
        System.exit(-1)
      }
    }

    log.info("Flatten JSON completed successfully for feed - " + feed)

    //generate response json for flatten json
    var status = "Flatten JSON and meta file generation for feed : " + feed + " completed successfully"
    Utils.handleException(feed, applicationId, 0, "SUCCESS", status, status, output)

    // close spark to enable copy  of hdfs files to S3
    spark.close

    //copy data to S3
    try {
      Utils.copyFromHdfsToS3(flatten.tempOutDir, ".*snappy", flatten.outDir)
    } catch {
      case e: Exception => {
        status = "Copy of data files from hdfs to S3 for feed - " + feed + " failed with error" + e.getMessage
        Utils.handleException(feed + "_data_s3copy", "", -1, "FAILED", status, status, output)
        log.error(status)
        log.error(e.printStackTrace)
        System.exit(-1)
      }
    }

    // copy meta to S3
    try {
      Utils.copyFromHdfsToS3(flatten.tempOutDir, ".*meta", flatten.outDir)
    } catch {
      case e: Exception => {
        status = "Copy of meta file from hdfs to S3 for feed - " + feed + " failed with error" + e.getMessage
        Utils.handleException(feed + "_meta_s3copy", "", -1, "FAILED", status, status, output)
        log.error(status)
        log.error(e.printStackTrace)
        System.exit(-1)
      }
    }

    //generate s3copy response json
    status = "Copy of both data and meta files from hdfs to S3 for feed : " + feed + " completed successfully"
    Utils.handleException(feed + "_s3copy", "", 0, "SUCCESS", status, status, output)
  }
}
