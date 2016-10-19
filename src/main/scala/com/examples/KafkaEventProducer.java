package com.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.commons.codec.binary.Base64;

/**
 * Created by user on 8/4/14.
 */
public class KafkaEventProducer {
     static String TOPIC = "log";
     static int part = 5;
     static int noofevents = 5;
     static String brokers= "localhost:9092";
     
  
     
     /**
      * Encodes the byte array into base64 string
      *
      * @param imageByteArray - byte array
      * @return String a {@link java.lang.String}
      */
     public static String encodeImage(byte[] imageByteArray) {
         return Base64.encodeBase64URLSafeString(imageByteArray);
     }
  
     /**
      * Decodes the base64 string into byte array
      *
      * @param imageDataString - a {@link java.lang.String}
      * @return byte array
      */
     public static byte[] decodeImage(String imageDataString) {
         return Base64.decodeBase64(imageDataString);
     }
     
     public static String getImage() {
    	 
         File file = new File("/res/Rainbow.jpg");
         String imageDataString = null;
         try {            
             // Reading a Image file from file system
             FileInputStream imageInFile = new FileInputStream(file);
             byte imageData[] = new byte[(int) file.length()];
             imageInFile.read(imageData);
  
             // Converting Image byte array into Base64 String
              imageDataString = encodeImage(imageData);
             
//  
//             // Converting a Base64 String into Image byte array
//             byte[] imageByteArray = decodeImage(imageDataString);
//  
//             // Write a image byte array into file system
//             FileOutputStream imageOutFile = new FileOutputStream(
//                     "/Users/jeeva/Pictures/wallpapers/water-drop-after-convert.jpg");
//  
//             imageOutFile.write(imageByteArray);
//  
//             imageInFile.close();
//             imageOutFile.close();
//  
//             System.out.println("Image Successfully Manipulated!");
         } catch (FileNotFoundException e) {
             System.out.println("Image not found" + e);
         } catch (IOException ioe) {
             System.out.println("Exception while reading the Image " + ioe);
         }
         return imageDataString;
     }
     
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ProducerConfig config = new ProducerConfig(props);
        
        Producer<String, String> producer = new Producer<String, String>(config);
 
        
        String content = "192.168.141.29	2016-10-05 17:11	PUT	/addItem	400	Chrome	Text	 0 HelloExample:229 - This is info : Text	";
//        for (int i = 0; i <= noofevents; i++) {
//        	String partName = TOPIC+"-" + i*part;
//        	if(i%3==0){
//        	//	message = TOPIC+"-" + i*part+"--"+getImage();
//        	}
//        	KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, partName, content);
//            
//        	producer.send(data);
//        }
        
        for (long nEvents = 0; nEvents < noofevents; nEvents++) { 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, TOPIC+"-"+String.valueOf(new Random().nextInt(5)), content);
               producer.send(data);
        }
        producer.close();
    }

//    public static void main(String[] argv){
//        Properties properties = new Properties();
//        properties.put("metadata.broker.list","localhost:9092");
//        properties.put("serializer.class","kafka.serializer.StringEncoder");
//        
//        ProducerConfig producerConfig = new ProducerConfig(properties);
//        
//        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
//        
//        SimpleDateFormat sdf = new SimpleDateFormat();
//        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC,"12121Test me " + sdf.format(new Date()));
//        
//        producer.send(message);
//        producer.close();
//    }
}
