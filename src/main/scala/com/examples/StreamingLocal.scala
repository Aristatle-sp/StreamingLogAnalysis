package com.examples

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.streaming._
import org.apache.spark.sql.functions.unix_timestamp
import com.datastax.spark.connector.cql.CassandraConnector
import org.joda.time.DateTime
import java.util.Formatter.DateTime
import org.apache.spark.sql.SaveMode

object StreamingLocal {

  def main(args: Array[String]) {

  
    
  val conf = new SparkConf().setAppName("StreamingExample")
  .setMaster("local[*]")
  .set("spark.local.ip","127.0.0.1")
  .set("spark.driver.host","127.0.0.1")
  .set("spark.cassandra.connection.host", "127.0.0.1")
  .set("spark.cassandra.connection.keep_alive_ms", "60000");

 Streaming.setStreamingLogLevels();
 val directotyName = "/Users/username/Documents/Demo/1/";
 val hdfsURI = "";
 //val hdfsURI = "hdfs://localhost:9000";
  val sourcedir = "source/";
 val resultdir = "result/";
 val hadoopConf = new org.apache.hadoop.conf.Configuration()
 val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsURI), hadoopConf) 
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm")
    

    val n = 10
 
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
     
    val topics = Set("log")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topics)

    stream.print();
        val input =   stream.map(x =>x)
                     
    input.foreachRDD { rdd => 
      if(!rdd.isEmpty()){
          rdd.saveAsTextFile(hdfsURI+directotyName+resultdir+(System.currentTimeMillis).toString());
       }
    }
    
    
                    //.saveToCassandra("log", "topn_content_type_by_time", SomeColumns("time", "key","contenttype","count"));

                     
                     //a.	Top N URLs
    //input.filter { x => x.split("\t").length > 6 }.map(x => (x.split("\t")(1),x.split("\t")(2),x.split("\t")(3),x.split("\t")(4),x.split("\t")(5)));
    //filtered.map(x => (x._1+x._3,1)).reduceByKey(_+_).take(10);                
  
   // c.	User agent – how many chrome, apple etc.  
  //  val userAgent = filtered.map(x => (x._1+x._5,1)).reduceByKey(_+_).take(10);
                  
                  
   //d.	Internal server errors – auth errors, server errors 
   //filtered.filter { x => Integer.parseInt(x._4) >= 400 }.map(x => (x._1+"\t"+x._4,1)).reduceByKey(_+_).map(x => (x._1.split("\t")(1),x._2))take(10);
                  
  
//        // Define the Kafka parameters, broker list must be specified
//    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,anotherhost:9092")
//    // Define which topics to read from
//    val topics = Set("sometopic", "anothertopic")

  
    ssc.start()    
    ssc.awaitTermination()
  }
  
  
  
  
}
