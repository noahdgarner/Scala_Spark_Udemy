//import Spark
import org.apache.spark.{SparkContext}
//to disable redundant logs
import org.apache.log4j.{Level, Logger}

object AverageFriendsByAge {

  //helper to split a line of input into (age, numFriends) tuples
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    //Implicit returns a tuple in Scala
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {

    //So that only error or failure logs show in the terminal
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "FriendsByAge") //init our sc, sparkcontext
    //name each rdd after the computation acted on it
    val lines = sc.textFile("src/main/resources/RDDFiles/fakefriends.csv") //filter lines

    //applies parseLine function to each row in the RDD
    //returns RDD of type (A,B)
    val rdd = lines.map(x => parseLine(x));
    //We want to take a file, 4 columns, parse it to 2 columns of age and numfriends
    //take the tuple, map all ages to an amount of friends that we can add
    //and reduce it to ages and average number of friends

    val totalsByAge = rdd
      .mapValues(x => (x, 1))
      //for each key (age) ie 33 yr old, combine keys by doing this to their values
      //(x._1) means access first element, and add to another element in the RDD (y._1)
      //(x._2) means access the second element, and add to another element (y._2)
      //for future ref, .reduceByKey x,y means values added together with respect to
      //one partition on a worker on a Cluster, remember this for future
      .reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    // So now we have tuples of (age, (totalFriends, totalPeopleOfAge))
    // To compute the average we divide totalFriends / totalPeopleOfAge for each age.
    val averagesByAge = totalsByAge
      .mapValues(x => (x._1 / x._2))

    //Kicks off computation. Brings data  back from cluster, print to console
    val results = averagesByAge
      .collect()

    // Sort and print the final results.
    results.sorted
      .foreach(println)
  }
}
