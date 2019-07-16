package com.impetus.spark.udemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
//import org.apache.spark.sql.functions._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  System.setProperty("hadoop.home.dir", "D:\\Spark\\WinUtils")

  // Create a SparkContext using every core of the local machine, named RatingsCounter
  val sc = new SparkContext("local[*]", "RatingsCounter")
  val sqlContext = new SQLContext(sc)
  val spark = sqlContext.sparkSession
  import spark.implicits._

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Load up each line of the ratings data into a DF
    val ratingsCSV = spark.read.option("header", "true").csv("D:/Udemy/SparkScala/ml-latest-small/ratings.csv")
    val ratings = ratingsCSV.select("movieId", "rating")

    // Count up how many times each value (rating) occurs
    val results = ratings.groupBy("movieId").count().orderBy($"count".desc)

    //Load movies data in DF
    val moviesCSV = spark.read.option("header", "true").csv("D:/Udemy/SparkScala/ml-latest-small/movies.csv")
    var nameDict = sc.broadcast(moviesCSV)
    val result1 = results.join(nameDict.value, results("movieId") === moviesCSV("movieId"), "left_outer").select(results("movieId"), $"title", $"count")
    result1.show(false)
  }
}
