import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/** Find the movies with the most ratings.
  * This is the first program that shows a very useful
  * application of Spark. Taking two semi related data files,
  * and comboing them together to gain insights.
  * E.G. We combined unnamed ratings and ids with
  * ids and movie names
  * over a set of thousands upon thousands of movies.*/
object PopularMoviesNicer {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    // How you declare Map in Scala, empty map duh use ur brain
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("src/main/resources/ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
         //interesting mapping syntax (id, movieName)
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
     //tuple type (movieId, movieName)
     return movieNames
  }

  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")  
    
    // Create a broadcast variable of our ID -> movie name map
    // Purpose: To not traverse network more than once
    val nameDict = sc.broadcast(loadMovieNames)
    
    // Read in each rating line
    val lines = sc.textFile("src/main/resources/ml-100k/u.data")
    
    // Map to (movieID, 1) tuples
    val movies = lines
      .map(x => (x.split("\t")(1).toInt, 1))
    
    // Count up all the 1's for each movie
    val movieCounts = movies
      .reduceByKey( (x, y) => x + y )

    // Sort them, tuple is (movieID, counts)
    val sortedMovies = movieCounts
      .sortBy(_._2, false)
    
    // Fold in the movie names from the broadcast variable
    // note our movies tuple is (movieID, counts)
    val sortedMoviesWithNames = sortedMovies
      .map( x  => (nameDict.value(x._1), x._2))

    val results = sortedMoviesWithNames.take(10)
    
    results.foreach(println)
  }
  
}

