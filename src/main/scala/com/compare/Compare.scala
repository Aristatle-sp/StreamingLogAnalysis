package com.compare

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.examples.Streaming
import java.util.Date

object Compare {

  def main(args: Array[String]) {

    
val conf = new SparkConf().setAppName("Compare")
.setMaster(args(0))
.set("spark.local.ip","127.0.0.1")
.set("spark.driver.host","127.0.0.1")
.set("spark.cassandra.connection.host", "127.0.0.1")
.set("spark.cassandra.connection.keep_alive_ms", "6000");
   Streaming.setStreamingLogLevels();
val spark = new SparkContext(conf)
spark.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
val hiveContext = new org.apache.spark.sql.hive.HiveContext(spark)
hiveContext.setConf("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm")

val hdfsURI = "hdfs://localhost:9000";
val hadoopConf = new org.apache.hadoop.conf.Configuration()
val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsURI), hadoopConf)
val directotyName = "/user/username/";
val hiveDirName = hdfsURI+directotyName+"source/hive/";
val sqlDirName = hdfsURI+directotyName+"source/mysql/";
val cassandraDirName = hdfsURI+directotyName+"source/cassandra/";
val missingRecordsInCassandraDir = hdfsURI+directotyName+"diff/missingRecords-Cassandra/";
val additionalRecordsInCassandraDir = hdfsURI+directotyName+"diff/additionalRecords-Cassandra/";
val missingRecordsInSQLDir = hdfsURI+directotyName+"diff/missingRecords-SQL/";
val additionalRecordsInSQLDir = hdfsURI+directotyName+"diff/additionalRecords-SQL/";
val cassandra_schema ="compare"
val cassandra_table = "personaldetails"

try { hdfs.delete(new org.apache.hadoop.fs.Path(hdfsURI+directotyName+"source/"), true) } catch { case _ : Throwable => { } }
try { hdfs.delete(new org.apache.hadoop.fs.Path(hdfsURI+directotyName+"diff/"), true) } catch { case _ : Throwable => { } }
val query = " Age > 7"
val hiveSelectQuery = "select * from personaldetails";
val mySQLURL = "jdbc:mysql://localhost:3306/test";

///////////HIVE/////////
val starttime = System.currentTimeMillis
val hivesource = hiveContext.sql(hiveSelectQuery).rdd;
hivesource.saveAsTextFile(hiveDirName)
val hivecount = hivesource.count().toInt;
//println("--HIVE Total-"+hivecount);

//hivesource.collect().foreach(println)

//////CASSANDRA/////////////

val cassandraRows = spark.cassandraTable(cassandra_schema, cassandra_table)
                    //.where("country ='GB' and  age >7 ")
//cassandraRows.foreach(println)
                    
val cassandraData = cassandraRows.map(row => "["+row.get[String]("id")+","+row.get[String]("gender")+","+row.get[String]("nameset")+","+row.get[String]("title")+","+row.get[String]("givenname")+","+row.get[String]("middleinitial")+","+row.get[String]("surname")+","+row.get[String]("streetaddress")+","+row.get[String]("city")+","+row.get[String]("zipcode")+","+row.get[String]("country")+","+row.get[String]("countryfull")+","+row.get[String]("username")+","+row.get[String]("password")+","+row.getInt("age")+","+row.get[String]("tropicalzodiac")+","+row.get[String]("cctype")+","+row.getInt("cvv2")+","+row.get[String]("ccexpires")+","+row.get[String]("westernunion")+","+row.get[String]("moneygram")+","+row.get[String]("color")+","+row.get[String]("occupation")+","+row.get[String]("company")+","+row.get[String]("vehicle")+","+row.get[String]("domain")+","+row.get[String]("pounds")+","+row.get[String]("kilograms")+"]")
//cassandraData.collect().take(3).foreach(println);
cassandraData.saveAsTextFile(cassandraDirName)
val cassandracount = cassandraData.count().toInt;
//println("--CASSANDRA Total-"+cassandracount);
//cassandraData.collect().foreach(println)


////COMPARE
val hiveDataToCompare = spark.textFile(hiveDirName);
val cassandraDataToCompare = spark.textFile(cassandraDirName);
val mysqlDataToCompare = spark.textFile(sqlDirName);

hiveDataToCompare.persist(StorageLevel.MEMORY_AND_DISK);
cassandraDataToCompare.persist(StorageLevel.MEMORY_AND_DISK);
mysqlDataToCompare.persist(StorageLevel.MEMORY_AND_DISK);


////HIVE Vs CASSANDRA


//println("--Hive Vs Cassandra starttime--"+starttime)
val missingRecordsCassandra = hiveDataToCompare.subtract(cassandraDataToCompare)
missingRecordsCassandra.persist(StorageLevel.MEMORY_AND_DISK)
val missingRecordsCassandracount = missingRecordsCassandra.count()
//println("---Missing Records - Cassandra count---"+missingRecordsCassandracount)
//missingRecordsCassandra.collect().foreach(println)
missingRecordsCassandra.saveAsTextFile(missingRecordsInCassandraDir)

val additionalRecordsInCassandra = cassandraDataToCompare.subtract(hiveDataToCompare)
additionalRecordsInCassandra.persist(StorageLevel.MEMORY_AND_DISK)
val additionalRecordsInCassandracount = additionalRecordsInCassandra.count()
//println("---Additional Records -Cassandra count---"+additionalRecordsInCassandracount)
//additionalRecordsInCassandra.collect().foreach(println)
additionalRecordsInCassandra.saveAsTextFile(additionalRecordsInCassandraDir)

val endtime = System.currentTimeMillis
//println("--Hive Vs Cassandra endtime--"+endtime)




///////// Write to Summary and Diff tables //////
val run1_source1 = "Hive"
val run1_source2 = "Cassandra"
val run1_totalrecords = hivecount;
val run1_timetaken = (endtime-starttime)/1000 //seconds
val run1_time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(Calendar.getInstance().getTime());

val run1_runid = run1_time+"-"+run1_source1+"-Vs-"+run1_source2
val run1_diffs2missing = missingRecordsCassandracount
val run1_diffs2additional = additionalRecordsInCassandracount
val cassandra_summary_table = "summary"
val cassandra_diff_details_table = "diff_details"

val run1_summaryar = List(run1_runid,run1_source1,run1_source2,run1_totalrecords,run1_timetaken,run1_diffs2missing,run1_diffs2additional,run1_time)
val run1_summarydata = spark.parallelize(Seq(run1_summaryar))

run1_summarydata.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
.saveToCassandra(cassandra_schema, cassandra_summary_table, SomeColumns("runid","source1","source2","totalrecords","timetaken","diff_s2_missing","diff_s2_additional","time"))

val run1_s1_ids = missingRecordsCassandra.map(x=>x.substring(1).split(",")(0)).toArray().toList
val run1_s2_ids = additionalRecordsInCassandra.map(x=>x.substring(1).split(",")(0)).toArray().toList
val run1_diffar = List(run1_runid,run1_source1,run1_source2,run1_s1_ids,run1_s2_ids,run1_time)
val run1_diffdata = spark.parallelize(Seq(run1_diffar))
run1_diffdata.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5)))
.saveToCassandra(cassandra_schema, cassandra_diff_details_table, SomeColumns("runid","source1","source2","s1_ids","s2_ids","time"))


///////MYSQL///////////
val run2_starttime = System.currentTimeMillis
val mysqlcontext = new org.apache.spark.sql.SQLContext(spark)

val dataframe_mysql = mysqlcontext.read.format("jdbc").option("url", mySQLURL)
                      .option("driver", "com.mysql.jdbc.Driver")
                      .option("dbtable", "PersonalDetails")
                      .option("user", "validmysqlusername")
                      .option("password", "").load()
                      //.where(query)
                      .rdd
dataframe_mysql.saveAsTextFile(sqlDirName)
//dataframe_mysql.collect().foreach(println)   
//val hivecount = dataframe_mysql.count().toInt;
//println("-SQL-"+dataframe_mysql.count())  


////CASSANDRA Vs MySQL

val missingRecordsInMySQL = cassandraDataToCompare.subtract(mysqlDataToCompare)
missingRecordsInMySQL.persist(StorageLevel.MEMORY_AND_DISK)
val missingRecordsInMySQLcount =missingRecordsInMySQL.count()

//println("---missingRecordsInMySQL---"+missingRecordsInMySQLcount)
//missingRecordsInMySQL.collect().foreach(println)
missingRecordsInMySQL.saveAsTextFile(missingRecordsInSQLDir)

val additionalRecordsInSQL = mysqlDataToCompare.subtract(cassandraDataToCompare)//.map (x=> "["+x+"]")
additionalRecordsInSQL.persist(StorageLevel.MEMORY_AND_DISK)
val additionalRecordsInSQLcount = additionalRecordsInSQL.count()
//println("---additionalRecordsInSQL---"+additionalRecordsInSQLcount)
additionalRecordsInSQL.saveAsTextFile(additionalRecordsInSQLDir)
val run2_endtime = System.currentTimeMillis


///////// Write to Summary and Diff tables //////
val run2_source1 = "Cassandra"
val run2_source2 = "MySQL"
val run2_totalrecords = cassandracount;
val run2_timetaken = (run2_endtime-run2_starttime)/1000 // seconds
val run2_time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(Calendar.getInstance().getTime());

val run2_runid = run2_time+"-"+run2_source1+"-Vs-"+run2_source2
val run2_diffs2missing = missingRecordsInMySQLcount
val run2_diffs2additional = additionalRecordsInSQLcount


val run2_summaryar = List(run2_runid,run2_source1,run2_source2,run2_totalrecords,run2_timetaken,run2_diffs2missing,run2_diffs2additional,run2_time)
val run2_summarydata = spark.parallelize(Seq(run2_summaryar))

run2_summarydata.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
.saveToCassandra(cassandra_schema, cassandra_summary_table, SomeColumns("runid","source1","source2","totalrecords","timetaken","diff_s2_missing","diff_s2_additional","time"))

val run2_s1_ids = missingRecordsInMySQL.map(x=>x.substring(1).split(",")(0)).toArray().toList
val run2_s2_ids = additionalRecordsInSQL.map(x=>x.substring(1).split(",")(0)).toArray().toList
val run2_diffar = List(run2_runid,run2_source1,run2_source2,run2_s1_ids,run2_s2_ids,run2_time)
val run2_diffdata = spark.parallelize(Seq(run2_diffar))
run2_diffdata.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5)))
.saveToCassandra(cassandra_schema, cassandra_diff_details_table, SomeColumns("runid","source1","source2","s1_ids","s2_ids","time"))


println("---Completed---")
spark.stop()
  }
}



//////DELETE BAD RECORDS FROM CASSANDRA/////
//
//val connector = CassandraConnector(conf)
//additionalRecordsInCassandra.foreachPartition(partition => {
//    val session: Session = connector.openSession //once per partition
//    partition.foreach{elem => 
//      println("----"+elem.substring(1).split(",")(0).toInt);  
//      val delete = s"DELETE FROM "+cassandra_schema+"."+cassandra_table+" where     id="+elem.substring(1).split(",")(0).toInt+";"
//        session.execute(delete)
//    }
//    session.close()
//})
//
////missingRecordsCassandra.map(x=>x.substring(1,x.length()-1)).map(x=>{
////  val m = x.split(","); 
////  m(0),m(10),m(14)
//
//missingRecordsCassandra.map(x=>x.substring(1,x.length()-1)).map(x =>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6),x.split(",")(7),x.split(",")(8),x.split(",")(9),x.split(",")(10),x.split(",")(11),x.split(",")(12),x.split(",")(13),x.split(",")(14),x.split(",")(15),x.split(",")(16),x.split(",")(17),x.split(",")(18),x.split(",")(19),x.split(",")(20)))//,x.split(",")(20),x.split(",")(20),x.split(",")(20),x.split(",")(20),x.split(",")(20),x.split(",")(20),x.split(",")(20)))
//.saveToCassandra(cassandra_schema, cassandra_table, SomeColumns("id","gender","nameset","title","givenname","middleinitial","surname","streetaddress","city","zipcode","country","countryfull","username","password","age","tropicalzodiac","cctype","cvv2","ccexpires","westernunion"))
//// SomeColumns("id","gender","nameset","title","givenname","middleinitial","surname","streetaddress","city","zipcode","country","countryfull","username","password","age","tropicalzodiac","cctype","cvv2","ccexpires","westernunion","moneygram","color","occupation","company","vehicle","domain" , "pounds" , "kilograms"))
