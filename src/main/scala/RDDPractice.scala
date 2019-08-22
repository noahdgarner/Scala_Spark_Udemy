//import Spark
import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Files, Paths}

import scala.collection.mutable.ArrayBuffer
//to disable redundant logs
import org.apache.log4j.{Level, Logger}

object RDDPractice {

  //driver program: The Spark Application's main method called the Driver
  def main(args: Array[String]) {
    //INTRO AND SET UP //
    //So that only error or failure logs show in the terminal
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Word Counts").set("spark.cores", "8") //init a Spark configuration, setting master and appname
    val sc = new SparkContext(conf) //init our sc, sparkcontext
    //Do some operations with our sparkContext
    val linesTextfileRDD = sc.textFile("src/main/resources/shakespeare.txt") //filter lines
    //the following is a transformation and then action (to 1 val) because we filter data that matches a predicate, this predicate is and
    println(linesTextfileRDD.filter(_.contains("and")).count + " is the number of lines that contain 'and' ")
    // _.contains("and") is a predicate function, for each element, if the element passes the predicate function
    //it stays in the new RDD

    //uses aggregation, first function does an intra partition, second does interpartition
    val flowers = sc.parallelize(List(11, 12, 13, 24, 25, 26, 35, 37, 24, 15, 16, 17), 3)
    //aggregation is simply doing 2 reduces. The first is on the intra partition
    //the second is on the interpartitions.
    //First is basically finding max of all in 1 partition, then it is saying
    //we will sum all the maxes across the partitions (inter)
    //the zero value will be returned if we call Math.min because it is
    //seeded across every partition
    val total = flowers.aggregate(0)(Math.max, _ + _)
    println("Total flowers: " + total)

    //We can also use sparkContext parallelize() method, which allows you to create your own RDD
    //notice we arent used sc.textFile, also note: parallelize is not used on large data sets. only for local testing
    val linesParallelizeRDD = sc.parallelize(List("I love doggos", "pandas r cool", "i like pandas!!!", "i like pandas"))
    linesParallelizeRDD.foreach(println) //note, this doesn't print to console because its executed on local nodes
    val pandaLinesRDD = linesParallelizeRDD.filter(_.contains("pandas")) //transform
    //notice, we Transform an RDD, which kicks off a computation. such as count(), first()
    println(pandaLinesRDD.first.replaceAll("^\\s+", ""))
    println(pandaLinesRDD.count + " is the number of lines that contain 'pandas' ")
    println(pandaLinesRDD.first().replaceAll("^\\s+", ""))
    println(linesParallelizeRDD.count)

    //example of sending an array of integers to a sparkContext to create an RDD from an array
    val data = Array(1, 2, 3, 4, 5)
    val distDataRDD = sc.parallelize(data) //passinga collection into the parallelize method
    distDataRDD.foreach(println)
    //how we print contents of our RDD
    val sumOfData = distDataRDD.reduce(_ + _) //action to sum all the numbers in distDataRDD
    println(sumOfData + " is the sum of all values in distData")
    //e.g. 3, textFile again, good example of how we can debug.
    val shakespeareTextPath = "src/main/resources/shakespeare.txt"
    val linesTextfileExample2 = sc.textFile(shakespeareTextPath)
    //Remember lineLength here, is computed lazily, meaning not till it needs to be
    val lineLengths = linesTextfileExample2.map(_.length) // map each line of strings  to an integer(its length), note:=>another RDD of only integers
    val totalLength = lineLengths.reduce(_ + _) //since we reduce here, we get a singly computed value of meaning
    println(totalLength + " total length if all the strings in the file were lined up 'counts all the chars'")

    if (!Files.exists(Paths.get("src/main/resources/lineLengths"))) //use this, haha nice
      lineLengths.saveAsTextFile("src/main/resources/lineLengths")

    //Working with key value pairs
    //This reduceByKey operation on KV pairs to count how many times each line of text occurs in a file
    val pairs = linesParallelizeRDD.map(s => (s, 1)) //maps every string to the number 1
    val counts = pairs.reduceByKey(_ + _) //every similar key (string) has its value added to itself
    counts.foreach(println) //counts is still a transformed RDD at this point, so we use a foreach to iterate through each KV pair
    println("Using collect() to convert RDD to an array of tuples, then calling .foreach().println")
    val array = pairs.collect() //collect will convert RDD into an array
    array.foreach(println)
    println("Calling foreach() as an action on RDD pairs.")
    pairs.foreach(println)
    println("Notice there is no difference")

    //simple test to show that a union between 2 RDDs can be accomplished
    val morePairsRDD = linesParallelizeRDD.map(x => (x, x.length)).persist //an RDD of the string(sentence), and an integer of the strings length
    val evenMorePairsRDD = linesParallelizeRDD.map(x => (x, 10)).persist //notice diff, above has a predicate on x
    val unionPairsRDD = morePairsRDD.union(evenMorePairsRDD).persist
    unionPairsRDD.foreach(println)
    val backToMorePairsRDD = unionPairsRDD.subtract(evenMorePairsRDD)
    backToMorePairsRDD.foreach(println) //gives us back our morePairsRDD from before the union

    val morePairsPath = "src/main/resources/MorePairsRDD"
    if (!Files.exists(Paths.get(morePairsPath))) //simply run only if relative path doesnt exist
      backToMorePairsRDD.saveAsTextFile(morePairsPath)
    //remember, 2 \\ cause delimeter
    //if we want to make sure this all goes to one file, just save the RDD as an Array
    //OR we can be super cool and use coalesce!!
    val coalescedFilePath = "src/main/resources/CoalescedFile"
    if (!Files.exists(Paths.get(coalescedFilePath)))
      backToMorePairsRDD.coalesce(1, true).saveAsTextFile(coalescedFilePath)


    val persistTestRDD = linesParallelizeRDD.map(x => (x, x.length)).persist
    println("contents of persistTestRDD after mapping")
    persistTestRDD.foreach(println) //notice it now prints a pair of tuples
    val firstLineRDD = persistTestRDD.take(1) //RDD not re-executed i.e. not mapped again
    val numElements = persistTestRDD.count //RDD is not re-executed, i.e. not mapped again

    //here is an example of fusing transformations together, called chaining
    val fuseTest = linesParallelizeRDD.map(_.toLowerCase).filter(_.contains("i")).take(2) //take returns an array
    println("Element 1:" + fuseTest(0) + "\nElement 2:" + fuseTest(1))


    //research again its been a year
    val curried = linesParallelizeRDD.aggregate(0)((x, _) => x + 1, _ + _)
    println(curried)


    for (i <- 0 to 10)
      println(i)


    val mutableArray = ArrayBuffer[Integer](1, 2, 3, 4, 5)
    mutableArray.insert(5, 6)

    for (i <- 0 to mutableArray.size - 1)
      println(mutableArray(i))

    //print each
    val immutableArray = Array(1, 2, 3, 4, 5)
    immutableArray.foreach(println);
    //access data in array
    immutableArray(0);

    var x = 5;
    while (x > 0) {
      println("hi")
      x -= 1;
    }

    //example iterate over collections
    val a = Array(1, 2, 3, 4)
    for (e <- a)
      print(e)

    //ie. a predicate
    def isEven(i: Int) = i % 2 == 0

    //pass the predicate and return what qualifies
    val filteredList = List(1, 2, 3, 4, 5, 6, 7, 8, 9).filter(isEven)
    println(filteredList);
  }
}



