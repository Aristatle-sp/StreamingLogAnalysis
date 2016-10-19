package com.examples;


import java.util.Random;
import java.util.regex.Pattern;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port topic1,topic2
 */

public final class JavaTest {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {

	 String str = "CREATE TABLE IF NOT EXISTS PersonalDetails(	Gender text(10),	NameSet text(30),	Title text(7),	GivenName text(20),	MiddleInitial text(1),	Surname text(20),	StreetAddress text(40),	City text(20),	State text(5),	StateFull text(20),	ZipCode text(10),	Country text(5),	CountryFull text(30),	EmailAddress text(40),	Username text(20),	Password text(15),	BrowserUserAgent text(20),	Telephone text(15),	MothersMaiden text(20),	Birthday timestamp,	Age int,	TropicalZodiac text(15),	CCType text(15),	CCNumber text(15),	CVV2 int,	CCExpires text(15),	NationalID text(10),	UPS text(30),	WesternUnion text(20),	MoneyGram text(20),	Color text(10),	Occupation text(30),	Company text(30),	Vehicle text(30),	Domain text(20),	BloodType text(5),	Pounds double,	Kilograms double,	FeetInches text(10),	Centimetres double,	GUID text(10),	Latitude double,	Longitude double) COMMENT 'PersonalDetails' ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ LINES TERMINATED BY '\n' STORED AS TEXTFILE";
	 		  for(int i =0 ;i<10;i++){
		  Random rand = new Random(); 
		  System.out.println("log-0"+String.valueOf(new Random().nextInt(5)));
	  }
  }
}