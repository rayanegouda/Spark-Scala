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


object Run {
def main(args: Array[String]) {
System.err.println("------------------------------------------------------------");
System.err.println("------------------------------------------------------------");
System.err.println("************************************************************");
System.err.println("Enter \"spark-core\" for spark-core")
System.err.println("************************************************************");
System.err.println("Enter \"producer consumer spark-streaming kafka\" for spark-streaming with kafka");
System.err.println("************************************************************");
System.err.println("Enter \"sparkMlib\" for sparkMlib");
System.err.println("************************************************************");
System.err.println("------------------------------------------------------------");
System.err.println("------------------------------------------------------------");
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
                  val prod = new Thread(new Runnable {
                  def run() {
                  //Execute the data producer
                  val prodprop = new Properties()
                  prodprop.load(new FileInputStream("src/main/resources/ProducerKafka.properties")) 
                  val producer = new ScalaKafkaProducer( prodprop ) 
                  producer.sendMessage()
                    }
                  }).start()
                  val cons = new Thread(new Runnable {
                  def run() {
                  //Execute the data spark-streaming consumer
                  val consprop = new Properties()
                  consprop.load(new FileInputStream("src/main/resources/ConsumerSparkStream.properties")) 
                  val consumer = new ConsumerSparkStreaming( consprop ) 
                  consumer.consume();
                    }
                  }).start()
               

                   // catch the default with a variable so you can print it
               case whoa  => println("Unexpected case: " + whoa.toString)
                
                }
     }
}