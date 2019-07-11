package com.impetus.spark.mavenProject

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

/** Usage: FlattenJSON <inputPath> <JSONOutputPathWithFileName> <CSVOutputPathWithFileName> */
object flattenScala {

  //System Properties
  System.setProperty("hadoop.home.dir", "D:\\Spark\\WinUtils")
  System.setProperty("spark.driver.allowMultipleContexts", "true")

  //Set Log Level
  val logger = Logger.getLogger("org")
  val log: Logger = LogManager.getLogger(flattenScala.getClass)

  //Spark Conf Settings
  val conf = new SparkConf().setAppName("flattenJSON").setMaster("local[*]").set("spark.driver.bindAddress", "127.0.0.1")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val spark = sqlContext.sparkSession
  val fs = FileSystem.get(sc.hadoopConfiguration)
  import spark.implicits._

  /** Flatten JSON DataFrame*/
  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length

    for (i <- 0 to fields.length - 1) {
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name

      log.info("Field is: " + field)
      log.info("fieldType is: " + fieldtype)
      log.info("fieldName is " + fieldName)

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

  /** Save Flattened DF as JSON*/
  def saveFlatJSON(df: DataFrame, tempOutputPath: Path, finalJSONPath: Path): Unit = {
    df.write.json(tempOutputPath.toString)
    val file = fs.globStatus(new Path("../output/1/part*"))(0).getPath().getName()
    fs.rename(new Path(tempOutputPath.toString + "/" + file), finalJSONPath)
    fs.delete(tempOutputPath)
  }

  /** Read Flattened JSON file and store as csv*/
  def saveAsCSV(tempOutputPath: Path, finalJSONPath: Path, finalCSVPath: Path): Unit = {
    val df_CSV = sqlContext.read.json(finalJSONPath.toString())
    df_CSV.write.format("com.databricks.spark.csv").option("header", "true").save(tempOutputPath.toString)
    val file = fs.globStatus(new Path("../output/1/part*"))(0).getPath().getName()
    fs.rename(new Path(tempOutputPath.toString + "/" + file), finalCSVPath)
    fs.delete(tempOutputPath)
  }

  def main(args: Array[String]) {

    logger.setLevel(Level.ERROR)
    if (args.length < 3) {
      println("Usage: FlattenJSON <inputPath> <JSONOutputPathWithFileName> <CSVOutputPathWithFileName>")
      System.exit(1)
    }

    //Path Variables
    val inputPath = args(0)
    //val schemaPath = "D:/masterSchema.json"
    val tempOutputPath = new Path("../output/1/")
    val finalJSONPath = new Path(args(1))
    val finalCSVPath = new Path(args(2))
    
    //Get input JSON file and Schema
    //val df_MasterSchemaJSON = sqlContext.read.option("multiLine", true).json(schemaPath).schema
    //val df_InputJSON = sqlContext.read.option("multiLine", true).schema(df_MasterSchemaJSON).json(inputPath)
    val df_InputJSON = sqlContext.read.option("multiLine", true).json(inputPath)
    df_InputJSON.show(false)

    //Flatten Nested JSON
    val flattenedDataFrame = flattenDataframe(df_InputJSON)
    flattenedDataFrame.show(false)

    saveFlatJSON(flattenedDataFrame,tempOutputPath,finalJSONPath)
    saveAsCSV(tempOutputPath,finalJSONPath,finalCSVPath)
  }
}
