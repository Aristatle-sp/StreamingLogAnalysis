package com.examples;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.commons.codec.binary.Base64;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class TestProducer {
    public static void main(String[] args) {
    	
//      String brokers= "localhost:9092";
//    	String TOPIC = "log";
//	    int noPart = 5;
//	    int noofevents = 5;
//		String imagepath = /Users/username/Desktop/Rainbow.jpg
	        String brokers= args[0];
	    	String TOPIC = args[1];
		    int noPart = Integer.parseInt(args[2]);
		    int noofevents = Integer.parseInt(args[3]);
	    	String imagepath = args[4];  
	    	        Properties props = new Properties();
	    	        props.put("metadata.broker.list", brokers);
	    	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	    	       // props.put("partitioner.class", "example.producer.SimplePartitioner");
	    	      //  props.put("request.required.acks", "1");
	    	 
	    	        ProducerConfig config = new ProducerConfig(props);
	    	 
	    	        Producer<String, String> producer = new Producer<String, String>(config);
	    	        Random rnd = new Random();
	    	        String content = "	2016-10-05 17:11	PUT	/downLoadImage	400	Chrome	Image	 0 HelloExample:229 - This is info : Text	"+getImage(imagepath);
	    	        for (long nEvents = 0; nEvents <= noofevents; nEvents++) { 
	    	               String part = TOPIC+String.valueOf(rnd.nextInt(noPart));//rnd.nextInt(5); 
	    	               String ip = "192.168.141." + rnd.nextInt(255); 
	    	               String msg = ip+content;
	    	               KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, part, msg);
	    	               producer.send(data);
	    	        }
	    	        producer.close();
	    	    }
    
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
    
    public static String getImage(String filepath) {
   	 
        File file = new File(filepath);
        //File file = new File("/Users/username/Desktop/Rainbow.jpg");
        String imageDataString = null;
        try {            
            // Reading a Image file from file system
            FileInputStream imageInFile = new FileInputStream(file);
            byte imageData[] = new byte[(int) file.length()];
            imageInFile.read(imageData);
 
            // Converting Image byte array into Base64 String
             imageDataString = encodeImage(imageData);
            
// 
//            // Converting a Base64 String into Image byte array
//            byte[] imageByteArray = decodeImage(imageDataString);
// 
//            // Write a image byte array into file system
//            FileOutputStream imageOutFile = new FileOutputStream(
//                    "/Users/jeeva/Pictures/wallpapers/water-drop-after-convert.jpg");
// 
//            imageOutFile.write(imageByteArray);
// 
//            imageInFile.close();
//            imageOutFile.close();
// 
//            System.out.println("Image Successfully Manipulated!");
        } catch (FileNotFoundException e) {
            System.out.println("Image not found" + e);
        } catch (IOException ioe) {
            System.out.println("Exception while reading the Image " + ioe);
        }
        return imageDataString;
    }
	    	}
