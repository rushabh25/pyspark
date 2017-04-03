import co.theasi.plotly._
import org.apache.spark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}


/**
  * Created by rshah on 4/2/17.
  */
object Main {


  case class InputFileFormat (label: String, text: String)

  var inputFile = ""
  var outputFile = ""

  def processArgs(args: List[String]) : Unit = {
    args match {
      case Nil => return
      case "--inputFile" :: head :: tail => {
        inputFile = head
        processArgs(tail)
      }
      case "--outputFile" :: head :: tail => {
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
      textFile(inputFile).
      map(_.split("\t")).
      map(attributes => InputFileFormat(attributes(0), attributes(1))).
      toDS()

    val raw_data_transformed = raw_data.withColumn("label_new", when($"label" === "ham", 0.0).otherwise(1.0)).
      drop($"label").
      withColumnRenamed("label_new", "label")

    val Array(training_set, test_set) = raw_data_transformed.randomSplit(Array(0.8, 0.2))

    // tokenizer
    val tokenizer = new Tokenizer().
      setInputCol("text").
      setOutputCol("words")

    // Stop Words Remover
    val stopWordsRemover = new StopWordsRemover().
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("filtered_words")

    // Hashing TF
    val hashingTF = new HashingTF().
      setNumFeatures(50000).
      setInputCol(stopWordsRemover.getOutputCol).
      setOutputCol("hashed")

    // IDF
    val idf = new IDF().
      setInputCol(hashingTF.getOutputCol).
      setOutputCol("features")

    // Logistic Regression
    val lr = new LogisticRegression().
      setMaxIter(10).
      setRegParam(0.001)

    // pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, lr))

    val model = pipeline.fit(training_set)

    val output = model.transform(test_set)

    output.write.format("com.databricks.spark.csv").mode("overwrite").option("delimiter", "\t").
      save(outputFile)

    // around 97.09% accuracy
    //output.count = 1196
    //output.filter($"label" === $"prediction").count
    //res17: Long = 1102

  }

}
