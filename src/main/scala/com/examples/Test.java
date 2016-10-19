//package com.examples;
//
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.regex.Pattern;
//
//import kafka.serializer.StringDecoder;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//
//import scala.Tuple2;
//
///**
// * Consumes messages from one or more topics in Kafka and does wordcount.
// * Usage: JavaDirectKafkaWordCount <brokers> <topics>
// *   <brokers> is a list of one or more Kafka brokers
// *   <topics> is a list of one or more kafka topics to consume from
// *
// * Example:
// *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2
// */
//
//public final class Test {
//  private static final Pattern SPACE = Pattern.compile(" ");
//
//  public static void main(String[] args) {
////    if (args.length < 2) {
////      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
////          "  <brokers> is a list of one or more Kafka brokers\n" +
////          "  <topics> is a list of one or more kafka topics to consume from\n\n");
////      System.exit(1);
////    }
//
//    //StreamingExamples.setStreamingLogLevels();
//
////    String brokers = args[0];
////    String topics = args[1];
//
//    
//    String brokers = "localhost:9092";
//    String topics = "test";
//
//    
//    
//    // Create context with a 2 seconds batch interval
//    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[2]");
//
//    		sparkConf.set("spark.local.ip","127.0.0.1");
//    		sparkConf.set("spark.driver.host","127.0.0.1");
//
//    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
//
//    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
//    HashMap<String, String> kafkaParams = new HashMap<String, String>();
//    kafkaParams.put("metadata.broker.list", brokers);
//    kafkaParams.put("auto.offset.reset", "largest");
//    kafkaParams.put("enable.auto.commit", "false");
//  
//    
//    //kafkaParams.put("auto.offset.reset", "largest"); // Failure cases .. Starts at latesr offset..thus losing data during the failure period. At most once
//    
//    //kafkaParams.put("auto.offset.reset", "smallets"); // Failure cases .. Starts at smallest offset..thus duplicate of data during the failure period.
////At least once
//    
////    JavaPairReceiverInputDStream<String, String> directKafkaStream = 
////    	     KafkaUtils.createDirectStream(streamingContext,
////    	         [key class], [value class], [key decoder class], [value decoder class],
////    	         [map of Kafka parameters], [set of topics to consume]);
//    
//    // Create direct kafka stream with brokers and topics
//    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
//    	jssc,
//        String.class,
//        String.class,
//        StringDecoder.class,
//        StringDecoder.class,
//        kafkaParams,
//        topicsSet
//    );
//    JavaDStream<Object> count = messages.map(s -> s._2.split(" "));
//    count.print();
////
////    // Get the lines, split them into words, count the words and print
////    JavaDStream<String> lines = messages.map(x -> x._2);
////    
////    JavaDStream<String> words = lines.flatMap( line -> Arrays.asList(line.split(" ")));
////    
////    
////    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
////            .reduceByKey((x, y) -> x + y);
////    
////    wordCounts.print();
//
//    // Start the computation
//    jssc.start();
//    jssc.awaitTermination();
//  }
//}