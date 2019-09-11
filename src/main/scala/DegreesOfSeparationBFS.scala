import java.nio.charset.CodingErrorAction

import com.sundogsoftware.spark.DegreesOfSeparation.BFSNode
import org.apache.log4j._
import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}


object DegreesOfSeparationBFS {

  val startCharacterID = 5306  //Spiderman
  val targetCharacterID = 14   //ADAM 3,031 (who the fuck is this)

  def parseLine(line: String): BFSNode = {
    //split on any space
    val fields = line.split("\\s")
    val heroID = fields(0).toInt
    //syntax for creating mutable array just as lame as Java
    var connections: ArrayBuffer[Int] = ArrayBuffer()
    for (connection <- 1 to (fields.length - 1)) {
      //init Array of the 2nd through last fields of heroes connected
     connections += fields(connection).toInt
    }
    //init start color and "infinity" distance
    var color:String = "White"
    var distance:Int = 9999
    //if the heroID is the startCharacter ie Spiderman, init Grey and 0 dis
    if (heroID == startCharacterID) {
      color = "GRAY"
      distance = 0
    }
    // Tuple contains (ID, (connect1, connect2, connect3, etc), 9999, White)
    //.toArray connections Array from mutable -> immutable
    (heroID, connections.toArray, distance, color)
  }


  def main(args: Array[String]) = {

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val sc = new SparkContext("local[8]", "DegreesSeparation")
    //load our textFile superhero graph
    val textFile = "Marvel-graph.txt"
    val superHeroGraph = sc.textFile(s"src/main/resources/RDDFiles/$textFile")

    //setup accumulator to track when we are done





  }

}
