package com.impetus.spark.flattenUtility

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.internal._
import org.apache.log4j._
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.fs._
import java.util.Locale
import java.io.File
import sys.process._


/** Usage: FlattenJSON <inputPath> <JSONOutputPathWithFileName> <CSVOutputPathWithFileName> */
object flattenJSON {
  val log: Logger = LogManager.getLogger(flattenJSON.getClass)

  /** Flatten JSON DataFrame*/
  def flattenDataframe(sc: SparkContext, df: DataFrame): DataFrame = {

    val log: Logger = LogManager.getLogger(flattenJSON.getClass)
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
          return flattenDataframe(sc, explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replaceAll("[^a-zA-Z0-9_-]", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(sc, explodedf)
        case _ =>
      }
    }
    df
  }

  def renameOutputFile(spark: SparkSession, fs: FileSystem, tempOutputPath: Path, inputPath: String): Array[FileStatus] = {

    import spark.implicits._

    val dateFormat = "yyyyMMdd"
    val dateValue = spark.range(1).select(date_format(current_timestamp, dateFormat)).as[(String)].first
    val inputFileName = inputPath.split("\\.")(0)

    val listOfFileStatus = fs.globStatus(new Path(tempOutputPath, "part*"))
    val listOfFiles = listOfFileStatus.map(x => x.getPath.getName)

    try{
    for (i <- listOfFiles) {
      val partName = i.split("-")(0) + i.split("-")(1)
      val finalJSONPath = new Path(inputFileName + "_" + partName + "_" + dateValue + ".json.snappy")
      fs.rename(new Path(tempOutputPath.toString + "/" + i), finalJSONPath)
      log.info("Compressed and Flattened JSON moved to HDFS Path : " + finalJSONPath)
    }
    }
    catch {
      case ex: Exception =>
        log.error("There was an exception while renaming JSON file. {} {}","", ex.getMessage, ex)
     }
    val inputFilePath = new Path(inputPath)
    val listOfRenamedFiles = fs.globStatus(new Path(inputFilePath.getParent, "*snappy"))
    return listOfRenamedFiles
  }

  /** Save Flattened DF as JSON*/
  def saveFlatJSON(sc: SparkContext, spark: SparkSession, df: DataFrame, inputPath: String): Array[FileStatus] = {

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val tempOutputPath = new Path("/tmp/output/1/")

    if (fs.exists(tempOutputPath)) {
      fs.delete(tempOutputPath, true)
    }

    df.write.option("compression", "snappy").json(tempOutputPath.toString)
    val fileStatus = renameOutputFile(spark, fs, tempOutputPath, inputPath)
    fs.delete(tempOutputPath)
    return fileStatus
  }

}
