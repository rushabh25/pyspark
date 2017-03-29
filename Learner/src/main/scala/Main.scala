import org.apache.spark.sql.SparkSession

/**
  * Created by rshah on 3/28/2017.
  */
object Main {

  private val DEFAULT_NUM_EXECUTORS = "2"
  private val DEFAULT_EXECUTOR_MEMORY = "1g"
  private val DEFAULT_EXECUTOR_CORES = "1"
  private val DEFAULT_DRIVER_MEMORY = "1g"
  private val DEFAULT_APP_NAME = java.util.UUID.randomUUID.toString
  private val DEFAULT_MASTER = "local"

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.
      builder().
      master("yarn").
      appName(DEFAULT_APP_NAME).
      config(SparkConfiguration.NUM_EXECUTORS, DEFAULT_NUM_EXECUTORS).
      config(SparkConfiguration.DRIVER_MEMORY, DEFAULT_DRIVER_MEMORY).
      config(SparkConfiguration.NUM_CORES, DEFAULT_EXECUTOR_CORES).
      config(SparkConfiguration.EXECUTOR_MEMORY, DEFAULT_EXECUTOR_MEMORY).
      getOrCreate()

    _1_RatingsHistogram.ProcessData(spark)
  }
}
