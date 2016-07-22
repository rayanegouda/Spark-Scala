package main.scala.com.spark.scala.kafka.prodcons

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import java.util.Properties


class ConsumerSparkStreaming  (props : Properties) {
  
   def consume()  = {

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName(props.getProperty("AppName")).setMaster(props.getProperty("Master"))
    val ssc = new StreamingContext(sparkConf, Seconds(Integer.parseInt(props.getProperty("frequency"))));
    // Create direct kafka stream with brokers and topics
    val topicsSet = props.getProperty("topicsSet").split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> props.getProperty("metadata.broker.list"))
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
   }
}