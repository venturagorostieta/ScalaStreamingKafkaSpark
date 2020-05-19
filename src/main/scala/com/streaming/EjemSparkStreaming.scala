package com.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
/**
 * Create topic "kafka-test"
 * Run a producer  console .
 * Run as spark application
 * 
 * Using Spark as Direct Stream {inputDStream  and KafkaUtils}
 */
object EjemSparkStreamming {
  
  def main(args: Array[String]): Unit = {
      exampleStreamming()
  }
  
  def exampleStreamming(): Unit = {
    
      val conf = new SparkConf().setMaster("local[*]").setAppName("EjemSparkStreamming-kafka")
      val ssc = new StreamingContext(conf, Seconds(2))
      val topics = "kafka-test" // lista de Topic de Kafka
      val brokers = "sandbox-hdp.hortonworks.com:6667" // broker de Kafka
      val groupId = "0" // Identificador del grupo.
      
      // Create direct kafka stream with brokers and topics
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
        
      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
        
      val lines = messages.map(_.value)
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
      wordCounts.print()
      // Start the computation
      ssc.start()
      ssc.awaitTermination()
  } 
}