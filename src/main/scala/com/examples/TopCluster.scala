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

object TopCluster {

  def main(args: Array[String]) {

    
val conf = new SparkConf().setAppName("TopNCluster");
//.setMaster(args(0));
//.set("spark.local.ip","127.0.0.1")
//.set("spark.driver.host","127.0.0.1");

val spark = new SparkContext(conf)

spark.textFile(args(0))
            
            .saveAsTextFile(args(1)+(System.currentTimeMillis).toString());
spark.stop()
  }
}
