import java.nio.charset.CodingErrorAction

import scala.io.{Codec, Source}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.desc




object WordCountJson {

  final case class Review(businessID:String,
                          cool:Long,
                          date:String,
                          funny:Int,
                          reviewID:String,
                          stars:Double,
                          text:String,
                          useful:Int,
                          userID:String)

  def mapToObjectForDS(line: Row): Review = {
    Review(line(0).asInstanceOf[String], line(1).asInstanceOf[Long].toInt,
      line(2).asInstanceOf[String], line(3).asInstanceOf[Long].toInt,
      line(4).asInstanceOf[String], line(5).asInstanceOf[Double],
      line(6).asInstanceOf[String], line(7).asInstanceOf[Long].toInt,
      line(8).asInstanceOf[String])
  }




  def main(args: Array[String]): Unit = {

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WordCountJson")
      .master("local[8]")
      .getOrCreate

    //make our Dataset
    val wordsDF = spark
      .read
      .json("src/main/resources/RDDFiles/review.json")
      .cache

    import spark.implicits._

    wordsDF
      .show(4)

    val viewForSqlQueries = wordsDF
      .createOrReplaceTempView("Json_view")

    val wordsRDD = wordsDF.rdd


    val mappedRDD = wordsRDD
      .map(mapToObjectForDS)
      .toDS

    var x = 0
    val oneRow = wordsRDD.first()
    println(oneRow(0))
    println(oneRow(1))
    println(oneRow(2))
    println(oneRow(3))
    println(oneRow(4))
    println(oneRow(5))



    mappedRDD.show(5)


  }
}
