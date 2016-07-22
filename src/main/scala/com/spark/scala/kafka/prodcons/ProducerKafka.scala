package com.spark.scala.kafka.prodcons

import kafka.producer.ProducerConfig
import java.util.Properties
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
import org.omg.PortableServer.THREAD_POLICY_ID


class ScalaKafkaProducer  ( props : Properties ) {

 //Create producer and send messages nbevents time
  def sendMessage()  = {
  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)
  for (nbevents <- Range(0, Integer.parseInt(props.getProperty("nbevents")))) {
     val runtime = new Date().getTime();
     val topicsSet = props.getProperty("topicsSet").split(",").toList
   
     for( i <- 0 to (topicsSet.size-1)){
       val data = new KeyedMessage[String, String]( topicsSet(i) , "runtime :" + props.getProperty("message") );
        producer.send(data);
       System.out.println("This Message: " + "\"" + props.getProperty("message")+ "\"" +  " has been sent to topic: " + topicsSet(i)) 
     }
  }
  System.out.println("ALL MESSAGES HAVE BEEN SENT");
  producer.close();              
	}
  
}