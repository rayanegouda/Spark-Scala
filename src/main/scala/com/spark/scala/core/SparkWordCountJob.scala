package com.spark.scala.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.commons.io.FileUtils
import java.io.File
import java.util.Properties

class SparkWordCountJob (sc: SparkContext) {
  //reads data  and computes the results
  def run(t: String , outputFile: String)  = {
  val readsdata = sc.textFile(t)
  // Transform into word and count.
  val words = readsdata.flatMap(word => word.split(" "))
  val counts = words.map(word => (word, 1)).reduceByKey(_ + _)
  // Save the word count back out to a text file
  counts.saveAsTextFile(outputFile)
                
	}
	
  //Delete results folder if exists
  def deleteFile(proprietes: Properties) = {
   if (new File((proprietes.getProperty("Output").toString())).exists ()) {
   FileUtils.deleteDirectory (new File((proprietes.getProperty("Output").toString())));
   } 
    }
}

