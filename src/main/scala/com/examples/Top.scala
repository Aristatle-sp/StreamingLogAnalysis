package com.examples
/*
local
/Users/username/Documents/softwares/data/sparkData/input/*.tsv
/Users/username/Documents/softwares/data/sparkData/output/01
0
* 
*/
* 
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

object Top {

  def main(args: Array[String]) {

    
val conf = new SparkConf().setAppName("TopN")
.setMaster(args(0))
.set("spark.local.ip","127.0.0.1")
.set("spark.driver.host","127.0.0.1");

val spark = new SparkContext(conf)

spark.textFile(args(1))
            .map(x => (x.split("\t")(4),x.split("\t")(5)))
            .filter{case (key,value) => value.contains(args(3))}
            .map(x => (x._1,1))
            .reduceByKey(_ + _)
            .map(item => item.swap)
            .sortByKey(false).coalesce(1,true)
            //.saveAsTextFile(args(2));
            .saveAsTextFile("hdfs://localhost:9000/user/out");
spark.stop()
  }
}
