import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

object PopularSuperHeroes {

  //method to load superfriends into associative array (map)
  def loadSuperFriends(): Map[Int, String] ={
    val fileName = "Marvel-names.txt"
    // Handle character encoding issues for when on cluster:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //init our Map
    var superHeroesMap:Map[Int, String] = Map()
    //grab file data
    val superHeroDataLines = Source.fromFile(s"src/main/resources/RDDFiles/$fileName").getLines()
    //loop through lines, keep what we need
    for (line <- superHeroDataLines) {
      val fields = line.split('"')
        if (fields.length > 1) {
          superHeroesMap += (fields(0).trim.toInt -> fields(1))
      }
    }
    superHeroesMap
  }


  def main(args:Array[String])={

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularSuperHeroes")

    //We've loaded the data, now we need to broadcast
    val superHeroMapCast = sc.broadcast(loadSuperFriends)
    //load our textFile superhero graph
    val superHeroGraph = sc.textFile("src/main/resources/RDDFiles/Marvel-graph.txt")







  }
}
