import org.apache.spark._
import org.apache.log4j._

import scala.io.Source
import java.nio.charset.CodingErrorAction

import scala.io.Codec
import scala.math.sqrt

object MovieSimilarities {

  /*Parsing function for map method*/
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("src/main/resources/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    movieNames
  }

  //user defined types. For clearer syntax
  // RDD goes in like: (userID, (moveId, rating), (moveId, rating))
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def makeMovieIdPairsKey(userRatings: UserRatingPair) = {
    val pair1: MovieRating = userRatings._2._1
    val pair2: MovieRating = userRatings._2._2
    //return RDD like: ((movieId, movieId), (rating, rating))
    ((pair1._1, pair2._1), (pair1._2, pair2._2))
  }

  //note all filter predicates are Boolean in nature
  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }


  //the shorter the distance, the more similar they are
  def computeEuclideanDistance(ratingsPairs: RatingPairs): (Double, Int) = {
    //grab each pair
    var sumDifferencesSquared: Double = 0.0
    var numPairs: Int = 0
    for (pair <- ratingsPairs) {
      sumDifferencesSquared += math.pow((pair._1 - pair._2), 2)
      numPairs += 1
    }
    (math.sqrt(sumDifferencesSquared), numPairs)
  }


  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      //add up all the rating multiplications
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    (score, numPairs)
  }

  //keep data where ratings are > 3 stars (we don't want bad ratings)
  type RatingDetails = (Int, (Int, Double))

  def filterGoodRatings(userRating: RatingDetails): Boolean = {
    userRating._2._2 > 3
  }

  //RDD goes in like: ((movieId, movieId), (score, numRatings))
  type SimmedMovieRatings = ((Int, Int), (Double, Int))

  def filterPerQualityThreshold(moviePairScored: SimmedMovieRatings, movieID: Int, isCosine: Boolean): Boolean = {
    var scoreThreshold: Double = 0
    var coOccurenceThreshold: Double = 50.0
    if (isCosine) {
      scoreThreshold = 0.97
      coOccurenceThreshold = 50.0
    }
    else {
      scoreThreshold = 0
      coOccurenceThreshold = 50.0
    }
    val pair = moviePairScored._1
    val sim = moviePairScored._2
    (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
  }


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[8]", "MovieSimilarities")

    println("\nLoading movie names...")
    val nameDict = sc
      .broadcast(loadMovieNames)

    val data = sc
      .textFile("src/main/resources/ml-100k/u.data")

    // Map ratings to key / value pairs: (\ require escaping. s+ 1 or more
    val ratings = data
      .map(l => l.split("\\s+"))
      .map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    //now have  user (ID => (movie ID, rating))
    val goodRatings = ratings
      .filter(filterGoodRatings)

    // Emit every movie rated together by the same user.
    // Self-join to find every combination. This is how we get our pairs
    val joinedRatings = goodRatings
      .join(goodRatings)

    // RDD: userID => ((movieID, rating), (movieID, rating))
    // Filter out duplicate pairs
    // Filtering will remove lines we don't want
    val uniqueJoinedRatings = joinedRatings
      .filter(filterDuplicates)

    // RDD: userID => ((movieID, rating), (movieID, rating))
    // Now key by (movie1, movie2) pairs. Rid userId don't care
    // Whenever we map, we are changing structure of each line
    val moviePairs = uniqueJoinedRatings
      .map(makeMovieIdPairsKey)

    // RDD: (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs
      .groupByKey

    // RDD: ((movie1, movie2) = > (rating1, rating2), (rating1, rating2) ... )
    // Can now compute similarities. Cache to avoid re-computation
    val moviePairSimilarities = moviePairRatings
      .mapValues(computeCosineSimilarity)


    // Extract similarities for the movie we care about that are "good" results.
    if (args.length > 0) {

      val movieID: Int = args(0).toInt

      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     

      val filteredResults = moviePairSimilarities
        .filter(x => filterPerQualityThreshold(x, movieID, true))
        .cache

      //RDD: ((movie1, movie2), (score, numRatings))
      // Sort by quality score.
      val results = filteredResults
        .sortBy(_._2._2, false)
        .take(10)

      println("\nTop 10 similar movies for " + nameDict.value(movieID))
      for (result <- results) {
        val sim = result._2
        val pair = result._1
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._2
        if (similarMovieID == movieID) {
          similarMovieID = pair._1
        }
        println(nameDict.value(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }


    //my own method of similarity via Euclid
    val moviePairSimilaritiesEuclid = moviePairRatings
      .mapValues(computeEuclideanDistance)

    // Extract similarities for the movie we care about that are "good" results.
    if (args.length > 0) {
      val movieID: Int = args(0).toInt
      val filteredResultsEuclid = moviePairSimilaritiesEuclid
        .filter(x => filterPerQualityThreshold(x, movieID, false))
        .cache
      //RDD: ((movie1, movie2), (score, numRatings))
      // Sort by quality score.
      val results = filteredResultsEuclid
        .sortBy(_._2._2, false)
        .take(10)

      println("\nTop 10 similar movies for " + nameDict.value(movieID))
      for (result <- results) {
        val sim = result._2
        val pair = result._1
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._2
        if (similarMovieID == movieID) {
          similarMovieID = pair._1
        }
        println(nameDict.value(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }



    //keep WebUI alive
    readLine
    sc.stop
  }
}
