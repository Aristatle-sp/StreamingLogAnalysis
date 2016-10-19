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

object MainExample {

  def main(args: Array[String]) {

  
    
  val conf = new SparkConf().setAppName("Streaming")
  .setMaster("local[*]")
  .set("spark.local.ip","127.0.0.1")
  .set("spark.driver.host","127.0.0.1")
  .set("spark.cassandra.connection.host", "127.0.0.1")
  .set("spark.cassandra.connection.keep_alive_ms", "60000");

   Streaming.setStreamingLogLevels();
   
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm")
    
     /** Creates the keyspace and table in Cassandra. */
      CassandraConnector(conf).withSessionDo { session =>
        //session.execute(s"DROP KEYSPACE IF EXISTS log")
        session.execute(s"CREATE KEYSPACE IF NOT EXISTS log WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute(s"CREATE TABLE IF NOT EXISTS log.topn_url (url TEXT PRIMARY KEY, count INT)")
        session.execute(s"CREATE TABLE IF NOT EXISTS log.topn_url_by_time (key text,time timestamp, count int, url text,PRIMARY KEY (key, time))")
        session.execute(s"TRUNCATE log.topn_url")
        session.close()
      }
    val n = 10
 
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
     
    val topics = Set("log")

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topics)

   // stream.print();
    stream.map(x =>x._2).count().print();
   
    

     
    val input =   stream.map(x =>x._2)
                     .filter { x => x.split("\t").length > 6 };
                     
    val sourceCount =  input.foreachRDD { rdd => 
      if(!rdd.isEmpty()){
         rdd.saveAsTextFile("hdfs://localhost:9000/user/source/"+(System.currentTimeMillis).toString());
       }
    }

    
//    val sourceCount =  input.foreachRDD { rdd => 
//      rdd.isEmpty()
//    }
//    
//    if(sourceCount){
//      input.saveAsTextFiles("hdfs://username.corp.net:9000/user/username/source/","txt");
//    }
        
                                      
        val filtered =             input.map(x => (x.split("\t")(1),x.split("\t")(2),x.split("\t")(3),x.split("\t")(4),x.split("\t")(5))).cache();
                    
                     //a.	Top N URLs
                     val topNUrl = filtered.map(x => (x._1+"\t"+x._3,1)).reduceByKey(_+_)
                                        .transform(rdd =>{
                                           val list = rdd.sortBy(_._2, false).take(n)
                                           ssc.sparkContext.parallelize(list,1);
                                        })
                    // topNUrl.print();        
                     
                     
                      val topnurlcount = topNUrl.map(x => (x._1.split("\t")(1),x._2));
                      topnurlcount.print();
                     topnurlcount.foreachRDD { rdd => 
                      if(!rdd.isEmpty()){
                         CassandraConnector(conf).withSessionDo { session =>
                                          session.execute(s"TRUNCATE log.topn_url")
                                          session.close()
                         }
                      }
                  }

                                              topnurlcount.saveToCassandra("log", "topn_url", SomeColumns("url","count"))

//                     topNUrl.count().print()
//                   val topnurlcount = topNUrl.map(x => (x._1.split("\t")(1),x._2))
//                   topnurlcount.saveToCassandra("log", "topn_url", SomeColumns("url","count"))
                   
                     
//                                  val count =topNUrl.foreachRDD { rdd => 
//                          rdd.count()
//                      }
//                     
//                    println("empty--"+count.toString());
//             
//            if(count.toString() !=0){
//              println("emptymm--"+count.toString());
//                          CassandraConnector(conf).withSessionDo { session =>
//                          session.execute(s"TRUNCATE log.topn_url")
//                          session.close()
//             
//                }
//            }
//topNUrl.count().print()
//                   val topnurlcount = topNUrl.map(x => (x._1.split("\t")(1),x._2))
//                   topnurlcount.saveToCassandra("log", "topn_url", SomeColumns("url","count"))
//                   
//        
//
//                   
//                    val topNUrlResult =  topNUrl.map(x => (x._1.toString().split("\t")(0),x._1.replace("\t", ""),x._1.toString().split("\t")(1),x._2));
//                    topNUrlResult.saveToCassandra("log", "topn_url_by_time", SomeColumns("time", "key","url","count"));
//          
//               
//      
//                     
//                      
//                     // c.	User agent – how many chrome, apple etc.
//                     
//                     val userAgent = filtered.map(x => (x._1+"\t"+x._5,1)).reduceByKey(_+_)
//                                        .transform(rdd =>{
//                                           val list = rdd.sortBy(_._2, false).take(n)
//                                           ssc.sparkContext.parallelize(list,1);
//                                        })
//                     userAgent.print();           
//                     userAgent.map(x => (x._1.toString().split("\t")(0),x._1.replace("\t", ""),x._1.toString().split("\t")(1),x._2))
//                    .saveToCassandra("log", "topn_user_agent_by_time", SomeColumns("time", "key","useragent","count"));
//                     
//                   
//                     // c.	User agent – how many chrome, apple etc.
//                     
//                     val errors = filtered.filter { x => Integer.parseInt(x._4) >= 400 }.map(x => (x._1+"\t"+x._4,1)).reduceByKey(_+_)
//                                        .transform(rdd =>{
//                                           val list = rdd.sortBy(_._2, false).take(n)
//                                           ssc.sparkContext.parallelize(list,1);
//                                        })
//                     errors.print();           
//                     errors.map(x => (x._1.toString().split("\t")(0),x._1.replace("\t", ""),x._1.toString().split("\t")(1),x._2))
//                    .saveToCassandra("log", "topn_error_type_by_time", SomeColumns("time", "key","errortype","count"));
//                     

//                      //	b.	Content distribution – jpeg, video, text                     
//                     val contenttype = filtered.map( x => x._3.contains(".jpeg")).map(x => (x._1+"\t"+x._3,1)).reduceByKey(_+_)
//                                        .transform(rdd =>{
//                                           val list = rdd.sortBy(_._2, false).take(n)
//                                           ssc.sparkContext.parallelize(list,1);
//                                        })
//                     contenttype.print();           
//                     contenttype.map(x => (x._1.toString().split("\t")(0),x._1.replace("\t", ""),x._1.toString().split("\t")(1),x._2))
//                    .saveToCassandra("log", "topn_content_type_by_time", SomeColumns("time", "key","contenttype","count"));
                     

    
    

                   
                   
  //a.	Top N URLs
    //input.filter { x => x.split("\t").length > 6 }.map(x => (x.split("\t")(1),x.split("\t")(2),x.split("\t")(3),x.split("\t")(4),x.split("\t")(5)));
    //filtered.map(x => (x._1+x._3,1)).reduceByKey(_+_).take(10);                
  
   // c.	User agent – how many chrome, apple etc.  
  //  val userAgent = filtered.map(x => (x._1+x._5,1)).reduceByKey(_+_).take(10);
                  
                  
   //d.	Internal server errors – auth errors, server errors 
   //filtered.filter { x => Integer.parseInt(x._4) >= 400 }.map(x => (x._1+"\t"+x._4,1)).reduceByKey(_+_).map(x => (x._1.split("\t")(1),x._2))take(10);
                  
  
    ssc.start()    
    ssc.awaitTermination()
  }
  
  
  
  
}
