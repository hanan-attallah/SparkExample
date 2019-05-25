import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import com.typesafe.config._

object HiveExample {
  def main(args: Array[String]): Unit = { // configure spark
    Logger.getLogger("org").setLevel(Level.WARN)



  }
}