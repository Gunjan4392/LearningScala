# Scala
Scala Learnings



Udemy Course -- https://www.udemy.com/scala-and-spark-2-getting-started/learn/lecture/9702526#overview

Scala Online -- https://scalafiddle.io/

FileUtil -- https://commons.apache.org/proper/commons-io/javadocs/api-release/index.html?org/apache/commons/io/FilenameUtils.html

Using IntelliJIdea for execution..


*  Directed Acyclic Graph(DAG) Engine optimizes workflows in SPark SCALA.
*  By default, Spark creates one partition for each block of the file (blocks being 128MB by default in HDFS)
*  Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).


SPARK

•	Every Spark application consists of a driver program that runs the user’s main function and executes various parallel operations on a cluster. 
•	The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel.
•	A second abstraction in Spark is shared variables that can be used in parallel operations. By default, when Spark runs a function in parallel as a set of tasks on different nodes, it ships a copy of each variable used in the function to each task. Sometimes, a variable need to be shared across tasks, or between tasks and the driver program. Spark supports two types of shared variables: broadcast variables, which can be used to cache a value in memory on all nodes, and accumulators, which are variables that are only “added” to, such as counters and sums.
o	spark-shell
•	Broadcast Variable:
o	It’s a read-only global variable, which all nodes of a cluster can read. Think of them more like as a lookup variable.
o	Broadcast variables must be able to fit in memory on one machine. That means that they should NOT be anything super large, like a large table or massive vector.
•	Accumulators:
o	They are like a global counter variable where each node of the cluster can write values in to.
o	These are the variables that you want to keep updating as a part of your operation like for example while reading log lines, one would like to maintain a real time count of number of certain log type records identified.

Login Ambari:
	Maria_dev		-	Maria_dev
	Raj_ops		-	raj_ops
	
Note: 
•	Spark 2.2.0 is built and distributed to work with Scala 2.11 by default. 
•	To write a Spark application, you need to add a Maven dependency on Spark. 
•	Ctrl+Shift+F		Formatting in Scala IDE
•	There should be a “Manifest.inf” class in my scala code, which would tell my spark which the main class is and needs to be executed first.
•	Right click on Project in Scala IDE  Run as “Maven Build”  Clean install This performs two operations, first “Clean”  It would remove the files or jars present in target directory. Second. “Install”  This would create new jars for your code.
•	Move jar files from local to Sandbox machine:
    scp -P 2222 C:/Users/gunjan.khandelwal/workspace/SecondMavenProject/target/data_spark_examples.jar root@localhost:/opt/
•	spark-submit --class com.impetus.spark.examples.WordCount --master local data_spark_examples.jar /tmp/wc_data output
•	Both input and output mentioned in spark-submit above are hdfs paths..

Configurations:
•	spark.conf.get("spark.sql.warehouse.dir")
•	spark.catalog.listDatabases.show(false)
•	spark.catalog.listTables("default").show()
•	When spark does not fetch hive databases or tables. How to check:
o	spark.conf.get("spark.sql.warehouse.dir")
o	spark.catalog.listDatabases().show(false)
•	If both are set to spark, change to hive:
o	spark-shell --conf spark.hadoop.metastore.catalog.default=hive --conf spark.sql.warehouse.dir = /warehouse/tablespace/managed/hive



Printing elements of an RDD

Another common idiom is attempting to print out the elements of an RDD using rdd.foreach(println) or rdd.map(println). On a single machine, this will generate the expected output and print all the RDD’s elements. However, in cluster mode, the output to stdout being called by the executors is now writing to the executor’s stdout instead, not the one on the driver, so stdout on the driver won’t show these! To print all elements on the driver, one can use the collect() method to first bring the RDD to the driver node thus: rdd.collect().foreach(println). This can cause the driver to run out of memory, though, because collect() fetches the entire RDD to a single machine; if you only need to print a few elements of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).

Using Spark SQL

•	In previous versions of Spark, you had to create a SparkConf and SparkContext to interact with Spark, whereas in Spark 2.0 the same effects can be achieved through SparkSession, without expliciting creating SparkConf, SparkContext or SQLContext, as they’re encapsulated within the SparkSession.	

•	Sample 1:
o	spark.version
	wget https://raw.githubusercontent.com/roberthryniewicz/datasets/master/svepisodes.json -O /tmp/svepisodes.json -O /tmp/svepisodes.json
	hdfs dfs -put /tmp/svepisodes.json /tmp
o	val path = “/tmp/svepisodes.json”
o	val svEpisodes = spark.read.json(path)
o	svEpisodes.printSchema()
o	svEpisodes.show()
o	svEpisodes.createOrReplaceTempView(“svepisodes”)
o	val df = spark.sql(“select * from svepisodes order by season, number”)
o	df.show()
o	val df = spark.sql(“SELECT count(1) AS TotalNumEpisodes FROM svepisodes”)
o	df.show()
o	val df = spark.sql(“SELECT season, count(number) as episodes FROM svepisodes GROUP BY season”)
o	df.show()
o	import org.apache.spark.sql.functions._
o	val svSummaries = svEpisodes.select("summary").as[String]
o	val words = (svSummaries.flatMap(_.split("\\s+")).filter(_ != "").map(_.toLowerCase()) )
o	(words.groupByKey(value => value).count().orderBy($"count(1)" desc).show())
o	val stopWords = List("a", "an", "to", "and", "the", "of", "in", "for", "by", "at")
o	val punctuationMarks = List("-", ",", ";", ":", ".", "?", "!")
o	val wordsFiltered = (words.filter(!stopWords.contains(_)).filter(!punctuationMarks.contains(_)))
o	(wordsFiltered.groupBy($"value" as "word").agg(count("*") as "occurences").orderBy($"occurences" desc).show())


•	Sample 2:
•	DOWNLOAD THE DATASET:

o	wget https://raw.githubusercontent.com/hortonworks/data-tutorials/master/tutorials/hdp/dataFrame-and-dataset-examples-in-spark-repl/assets/people.json
o	wget https://raw.githubusercontent.com/hortonworks/data-tutorials/master/tutorials/hdp/dataFrame-and-dataset-examples-in-spark-repl/assets/people.txt
o	hdfs dfs -put people.txt /tmp/
o	hdfs dfs -put people.json /tmp/

•	DATAFRAME API EXAMPLE
o	val df = spark.read.json("/tmp/people.json")
o	df.show()
o	df.select(df("name"),df("age")+1).show()
o	df.filter(df("age") > 20).show()

•	PROGRAMMATICALLY SPECIFYING SCHEMA
•	Import the necessary libraries
o	import org.apache.spark.sql._
o	import org.apache.spark.sql.Row
o	import org.apache.spark.sql.types._
o	import spark.implicits._
•	Create and RDD
o	val peopleRDD = spark.sparkContext.textFile("/tmp/people.txt")
•	Encode the Schema in a string
o	val schemaString = "name age"
•	Generate the schema based on the string of schema
o	val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
o	val schema = StructType(fields)
•	Convert records of the RDD (people) to Rows
o	val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
•	Apply the schema to the RDD
o	val peopleDF = spark.createDataFrame(rowRDD, schema)
•	Creates a temporary view using the DataFrame
o	peopleDF.createOrReplaceTempView("people")
•	SQL can be run over a temporary view created using DataFrames
o	val results = spark.sql("SELECT name FROM people")
•	The results of SQL queries are DataFrames and support all the normal RDD operations. The columns of a row in the result can be accessed by field index or by field name
o	results.map(attributes => "Name: " + attributes(0)).show()

•	DATASET API EXAMPLE
o	case class Person(name: String, age: Long)
o	val ds = Seq(Person("Andy", 32)).toDS()
o	ds.show()
o	val path = "/tmp/people.json"
o	val people = spark.read.json(path).as[Person] // Creates a DataSet
o	people.show
o	val pplFiltered = people.filter("age is not null")
o	pplFiltered.show



Resilient Distributed Dataset (RDD)
•	RDD is an immutable distributed collection of elements of your data, partitioned across nodes in your cluster that can be operated in parallel with a low-level API that offers transformations and actions.
•	It allows a programmer to perform in-memory computations on large clusters in a fault-tolerant manner.
•	DataFrames and Datasets are built on top of RDDs.

DataFrames
•	Like an RDD, a DataFrame is an immutable distributed collection of data. Unlike an RDD, data is organized into named columns, like a table in a relational database. 
•	 DataFrame in Spark allows developers to impose a structure onto a distributed collection of data, allowing higher-level abstraction.
•	If you are trying to access the column which does not exist in the table in such case Dataframe APIs does not support compile-time error. It detects attribute error only at runtime.
Dataset 
•	It is an extension of DataFrame API that provides the functionality of – type-safe, object-oriented programming interface of the RDD API
•	It represents data in the form of JVM objects of row or a collection of row object. 
•	It provides compile-time type safety.
•	Spark converts your data into DataFrame = Dataset[Row], a collection of generic Row object, since it does not know the exact type.
•	The main difference between the Datasets and DataFrames is that Datasets are strongly typed, requiring consistent value/variable type assignments.



•	Sample 3:
	wget https://raw.githubusercontent.com/hortonworks/data-tutorials/master/tutorials/hdp/learning-spark-sql-with-zeppelin/assets/flights.csv -O /tmp/flights.csv
	cat /tmp/flights.csv | head
	hdfs dfs -put /tmp/flights.csv /tmp/
o	val flights = (spark.read.option("header","true").option("inferSchema","true").csv("/tmp/flights.csv"))
o	flights.printSchema()
o	flights.select("Year","Month","UniqueCarrier","FlightNum","DepDelay","ArrDelay","Distance").show()
o	flights.select("Year","Month","UniqueCarrier","FlightNum","DepDelay","ArrDelay","Distance").filter($"DepDelay" > 15).show()
o	val numTotalFlights = flights.count()
o	val numDelayedFlights = DepDelayedFlights.count()
o	println("% of delayed flights: "+(numDelayedFlights.toFloat/numTotalFlights * 100)+ "%")
•	UDF
o	import org.apache.spark.sql.functions.udf
o	val isDelayedUDF = udf((time: String) => if (time == "NA") 0 else if (time.toInt > 15) 1 else 0)
o	val flightsWithDelays = flights.select($"Year", $"Month", $"DayofMonth", $"UniqueCarrier", $"FlightNum", $"DepDelay", isDelayedUDF($"DepDelay").alias("IsDelayed"))
o	flightsWithDelays.show(5)

•	Use of agg() -- you have 2 different columns - and you want to apply different agg functions to each of them. agg on a Dataset is simply a shortcut for groupBy()
o	flightsWithDelays.agg((sum("IsDelayed") * 100 / count("DepDelay")).alias("Percentage of Delayed Flights")).show()
o	(flights.select("Origin", "Dest", "TaxiIn").groupBy("Origin", "Dest").agg(avg("TaxiIn").alias("AvgTaxiIn")).orderBy(desc("AvgTaxiIn")).show())

•	TempViews:
o	flights.createOrReplaceTempView("flightsView")
o	spark.sql("select * from flightsView").show()

•	Register a helper UDF. Note that this is a UDF specific for use with the sparkSession
o	spark.udf.register("isDelayedUDF", (time: String) => if (time == "NA") 0 else if (time.toInt > 15) 1 else 0)
o	spark.sql(“SELECT UniqueCarrier, SUM(isDelayedUDF(DepDelay)) AS NumDelays FROM flightsView GROUP BY UniqueCarrier”).show()
o	spark.sql(“SELECT UniqueCarrier, SUM(DepDelay) AS TotalTimeDelay FROM flightsView GROUP BY UniqueCarrier”).show()
o	spark.sql("select * from (SELECT DayOfWeek, CASE WHEN isDelayedUDF(DepDelay) = 1 THEN 'delayed' ELSE 'ok' END AS Delay, COUNT(1) AS Count FROM flightsView GROUP BY DayOfWeek, CASE WHEN isDelayedUDF(DepDelay) = 1 THEN 'delayed' ELSE 'ok' END ORDER BY DayOfWeek) as a order by Delay asc, Count desc ").show()



•	Sample 4 [RDD]:
	wget http://hortonassets.s3.amazonaws.com/tutorial/data/yahoo_stocks.csv
	hdfs dfs -put /tmp/yahoo_stocks.csv /tmp/yahoo_stocks.csv
o	spark-shell
o	import org.apache.spark.sql.SQLContext
o	import org.apache.spark.sql.types
o	import org.apache.spark.sql._
o	import spark.implicits._
o	val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
o	spark.sql("CREATE TABLE yahoo_orc_table (date STRING, open_price FLOAT, high_price FLOAT, low_price FLOAT, close_price FLOAT, volume INT, adj_price FLOAT) stored as orc")
o	val yahoo_stocks = sc.textFile("/tmp/yahoo_stocks.csv")
o	yahoo_stocks.take(10)
o	val header = yahoo_stocks.first
o	header
o	val data = yahoo_stocks.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
o	data.first
o	case class YahooStockPrice(date: String, open: Float, high: Float, low: Float, close: Float, volume: Integer, adjClose: Float)
o	val stockprice = data.map(_.split(",")).map(row => YahooStockPrice(row(0), row(1).trim.toFloat, row(2).trim.toFloat, row(3).trim.toFloat, row(4).trim.toFloat, row(5).trim.toInt, row(6).trim.toFloat)).toDF()
o	stockprice.first
o	stockprice.show
o	stockprice.printSchema
o	stockprice.createOrReplaceTempView("yahoo_stocks_temp")
o	val results = spark.sql("SELECT * FROM yahoo_stocks_temp")
o	results.map(t => "Stock Entry: " + t.toString).collect().foreach(println)
o	results.write.format("orc").save("yahoo_stocks_orc")
o	results.write.format("orc").save("/apps/hive/warehouse/yahoo_stocks_orc")
o	val yahoo_stocks_orc = spark.read.format("orc").load("yahoo_stocks_orc")
o	yahoo_stocks_orc.createOrReplaceTempView("orcTest")
o	spark.sql("SELECT * from orcTest").collect.foreach(println)
o	cat /tmp/flights.csv | head
o	hdfs dfs -put /tmp/flights.csv /tmp/


