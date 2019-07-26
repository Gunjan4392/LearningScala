package com.gkd.spark.flattenUtility

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.internal._
import org.apache.log4j._
import org.apache.commons.io.FilenameUtils._
import org.apache.hadoop.fs._
import sys.process._

/** Usage: FlattenJSON <inputPath> <JSONOutputPathWithFileName> <CSVOutputPathWithFileName> */
object flattenJSON {
  val log: Logger = LogManager.getLogger(flattenJSON.getClass)

  /** Flatten JSON DataFrame*/
  def flattenDataframe(df: DataFrame): DataFrame = {

    
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
          return flattenDataframe(explodedDf)

        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName + "." + childname)
          val newfieldNames = fieldNames.filter(_ != fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_").replaceAll("[^a-zA-Z0-9_-]", ""))))
          val explodedf = df.select(renamedcols: _*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }

  def renameOutputFile(spark: SparkSession, fs: FileSystem, inputPath: String, tempOutputPath: Path, outputPath: String): Array[FileStatus] = {

    import spark.implicits._

    val dateFormat = "yyyyMMdd"
    val dateValue = spark.range(1).select(date_format(current_timestamp, dateFormat)).as[(String)].first
    val inputFileName = getBaseName(inputPath)
    val listOfFileStatus = fs.globStatus(new Path(tempOutputPath, "part*")).map(x => x.getPath.getName)

    try {
      for (i <- listOfFileStatus) {
        val partName = i.split("-").slice(0, 2).mkString("")
        val finalJSONPath = new Path(outputPath + "/" + inputFileName + "_" + partName + "_" + dateValue + ".json.snappy")
        fs.rename(new Path(tempOutputPath.toString + "/" + i), finalJSONPath)
        log.info("Compressed and Flattened JSON moved to HDFS Path : " + finalJSONPath)
      }
    } catch {
      case ex: Exception =>
        log.error("There was an exception while renaming JSON file. {} {}", "", ex.getMessage, ex)
    }
    val listOfRenamedFiles = fs.globStatus(new Path(outputPath, "*snappy"))
    fs.delete(tempOutputPath)
    return listOfRenamedFiles
  }

  /** Save Flattened DF as JSON*/
  def saveFlatJSON(spark: SparkSession, df: DataFrame, inputPath: String, outputPath: String): Array[FileStatus] = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tempOutputPath = new Path(outputPath + "/output")
    df.write.option("compression", "snappy").mode("overwrite").json(tempOutputPath.toString)
    val fileStatus = renameOutputFile(spark, fs, inputPath, tempOutputPath, outputPath)
    return fileStatus
  }
}
