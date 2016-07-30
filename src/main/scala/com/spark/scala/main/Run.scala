package com.spark.scala.main

import com.spark.scala.core
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.spark.scala.core.SparkWordCountJob
import java.util.Properties
import java.io.FileInputStream
import com.spark.scala.core.SparkWordCountJob
import com.spark.scala.kafka.prodcons.ScalaKafkaProducer
import kafka.producer.ProducerConfig
import main.scala.com.spark.scala.kafka.prodcons.ConsumerSparkStreaming
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ SparseVector, DenseVector, Vector, Vectors }
import scala.util.control.NonFatal
import scala.collection.immutable.{ Map, HashMap }
import org.apache.spark.rdd.RDD
import com.spark.scala.mlib.randomforest.Titanic
import org.apache.commons.io.FileUtils
import java.io.File



object Run {
def main(args: Array[String]) {
System.err.println("******************************************************************************************");
System.err.println("******************************************************************************************");
System.err.println("**************     **************     **************     **************     ***        ***");
System.err.println("**************     **************     **************     **************     ***       *** ");
System.err.println("***                ***        ***     ***        ***     ***        ***     ***      ***  ");
System.err.println("***                ***        ***     ***        ***     ***        ***     ***     ***   ");
System.err.println("*************      **************     **************     **************     ***********   ");
System.err.println("          ***      **************     **************     **************     ***********   ");
System.err.println("          ***      ***                ***        ***     ***     ***        ***     ***   ");
System.err.println("*************      ***                ***        ***     ***       ***      ***      ***  ");
System.err.println("*************      ***                ***        ***     ***        ***     ***       *** ");
System.err.println("******************************************************************************************");
System.err.println("******************************************************************************************");
System.err.println("Enter \"1\" for spark-core")
System.err.println("******************************************************************************************");
System.err.println("Enter \"2\" for spark-streaming with kafka");
System.err.println("******************************************************************************************");
System.err.println("Enter \"3\" for sparkSQL and sparkDataFrame");
System.err.println("******************************************************************************************");
System.err.println("Enter \"4\" for sparkMlib");
System.err.println("******************************************************************************************");
System.err.println("Enter \"5\" for sparkGraphX");
System.err.println("******************************************************************************************");

    val scanner = new java.util.Scanner(System.in).nextLine();
    scanner match {
               case "1" =>  
                  val prop = new Properties()
                  prop.load(new FileInputStream("src/main/resources/sparkcore.properties")) 
                  val Input = prop.getProperty("Input")
                  val conf = new SparkConf().setAppName(prop.getProperty("AppName")).setMaster(prop.getProperty("Master"))
                  val context = new SparkContext(conf)
                  val job = new SparkWordCountJob(context)
                  job.deleteFile(prop)
                  val results = job.run(Input,prop.getProperty("Output")) 
                  context.stop()
                  
               case "2" =>
                  //Execute the data producer
                  val prod = new Thread(new Runnable {
                  def run() {
                  //Load Producer properties
                  val prodprop = new Properties()
                  prodprop.load(new FileInputStream("src/main/resources/ProducerKafka.properties")) 
                  val producer = new ScalaKafkaProducer( prodprop ) 
                  //Send data
                  producer.sendMessage()
                    }
                  }).start()
                  //Execute the data consumer spark-streaming
                  val cons = new Thread(new Runnable {
                  def run() {
                  //Execute the data spark-streaming consumer
                  val consprop = new Properties()
                  consprop.load(new FileInputStream("src/main/resources/ConsumerSparkStream.properties")) 
                  val consumer = new ConsumerSparkStreaming( consprop ) 
                  consumer.consume();
                    }
                  }).start()
                  
               case "3" =>
                  val prop = new Properties()
                  prop.load(new FileInputStream("src/main/resources/sparksql.properties")) 
                  val conf = new SparkConf().setAppName(prop.getProperty("AppName")).setMaster(prop.getProperty("Master"))
                  val context = new SparkContext(conf)
                  val sqlContext = new org.apache.spark.sql.SQLContext(context)
                  import sqlContext.implicits._
                  val df = sqlContext.jsonFile(prop.getProperty("testFile"))
                  // Displays the content of the DataFrame to stdout
                  df.show()
                  df.printSchema()
                  // Selecting countryname and filtering lendprojectcost >-57000000
                  df.select("countryname").show()
                  df.filter($"lendprojectcost" >= 5700000).show()
                  // Group by countryname and  count 
                  df.groupBy("countryname").count().show()
                  
               case "4" =>
                  // Create context with 2 second batch interval
                  val prop = new Properties()
                  prop.load(new FileInputStream("src/main/resources/sparkmlib.properties")) 
                  //Delete results folder if exists
                   if (new File((prop.getProperty("testResult").toString())).exists ()) {
                   FileUtils.deleteDirectory (new File((prop.getProperty("testResult").toString())));
                   } 
                       if (new File((prop.getProperty("trainResult").toString())).exists ()) {
                   FileUtils.deleteDirectory (new File((prop.getProperty("trainResult").toString())));
                     }                  
                  val conf = new SparkConf().setAppName(prop.getProperty("AppName")).setMaster(prop.getProperty("Master"))
                  val context = new SparkContext(conf)
                  // Load the "train.csv" file
                  // Filter out the header and empty line(s)
                  // Interpret each line into a Titanic
                  var trainCSV = (context.textFile(prop.getProperty("trainFile"))
                    .filter(line => !line.isEmpty() && line.charAt(0).isDigit)
                    .map(line => new Titanic(line)))
                  // Curate the data and transform it into a list of LabeledPoint
                  // TODO data curation here
                  var trainingData = (trainCSV
                    .map(record => record.toLabeledPoint()))
                  // Load the test.csv dataset
                  var testCSV = (context.textFile(prop.getProperty("testFile"))
                    // fiter out the header and empty line(s)
                    .filter(line => !line.isEmpty() && line.charAt(0).isDigit)
                    // interpert each line into a TitanicEntry
                    .map(line => new Titanic(line)))
                  // Curate the data and transform it into a list of (passengerId, Vector) tuples
                  var testData = (testCSV
                    // TODO data curation here
                    .map(record => (record.features("passengerId"), record.toVector())))
                  val numClasses = 2 //2 classes 1: for survived and 0: for died
                  // TODO enter the list of categorical features (like sex, embarked) and the number of elements
                  val categoricalFeaturesInfo = Map[Int, Int]()
                  
                  /*In general, the more trees you use the better get the results. However, the improvement 
                   * decreases as the number of trees increases, i.e. at a certain point the benefit in
                   *  prediction performance from learning more trees will be lower than the cost in 
                   *  computation time for learning these additional trees.
                   * */
                  val numTrees = 1000 
                  val featureSubsetStrategy = "auto"
                  
                  /* Gini impurity is a measure of how often a randomly chosen element from the set would be 
                   * incorrectly labeled if it was randomly labeled according to the distribution of labels 
                   * in the subset.
                   */
                  val impurity = "gini"
                  val maxDepth = 30 
                  val maxBins = 32
                  val model = RandomForest.trainClassifier(
                    trainingData, numClasses, categoricalFeaturesInfo,
                    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)      
                  val predictions = testData.map(t => {
                  val prediction = model.predict(t._2)
                    (t._1, prediction)
                  })
                  predictions.map(t => (t._1.toInt, t._2.toInt)).collect().foreach(t => println(t._1 + "," + t._2))
                  //merge predictions with original data
                  val predictionsMap = predictions.collect().toMap  
                  val finalTrainESData = trainCSV.map(_.loadData) 
                  val finalTestESData = testCSV.map { x => 
                    { 
                      val pid = x.features("passengerId")
                      x.loadData updated ("survived", predictionsMap(pid.toString()).toInt)
                    }} 
                  finalTestESData.saveAsTextFile(prop.getProperty("testResult"))
                  finalTrainESData.saveAsTextFile(prop.getProperty("trainResult"))
                  
               case "5" =>
              
                          
              
                                 // catch the default with a variable so you can print it
                             case whoa  => println("Unexpected case: " + whoa.toString)
                              
                              }
     }
}