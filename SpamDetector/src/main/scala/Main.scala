import co.theasi.plotly._
import org.apache.spark
import org.apache.spark.sql._
import org.viz.lightning._


/**
  * Created by rshah on 4/2/17.
  */
object Main {


  case class InputFileFormat (label: String, raw_data: String)

  var inputFile = ""
  var outputFile = ""

  def processArgs(args: List[String]) : Unit = {
    args match {
      case Nil => return
      case "--spamFile" :: head :: tail => {
        inputFile = head
        processArgs(tail)
      }
      case "--hamFile" :: head :: tail => {
        outputFile = head
        processArgs(tail)
      }
      case _ => {
        println("Do not recognize this command line parameter")
        System.exit(1)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // call processArgs function to populate the spamFile and hamFile parameters
    processArgs(args.toList)

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val raw_data = spark.sparkContext.
      textFile("/users/rshah/Documents/git-workspace/spark/SpamDetector/smsspamcollection/SMSSpamCollection").
      map(_.split("\t")).
      map(attributes => InputFileFormat(attributes(0), attributes(1))).
      toDS()

    val 

  }

}
