//import Spark
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
//to disable redundant logs
import org.apache.log4j.{Level, Logger}


object Test {
  System.setProperty("hadoop.home.dir", "C:\\winutil")
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Word Counts").set("spark.cores", "8") //init a Spark configuration, setting master and appname
    val sc = new SparkContext(conf)

    val linesTextfileRDD = sc.textFile("./src/main/resources/shakespeare.txt") //filter lines
    val andLines = linesTextfileRDD.filter(_.contains("and"))

    //bugs out currently, but why?
    println(andLines.count());
  }
}
