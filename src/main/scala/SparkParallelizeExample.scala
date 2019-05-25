
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import scala.util.parsing.json.JSON


object NewTest {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    var fifaDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(getClass.getResource("/FifaData.csv").getPath).toDF()
    fifaDF.createOrReplaceTempView("Fifa")

    //read the new salary from new file:
    var updatedSalariesDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(getClass.getResource("/UpdatedSalaries.csv").getPath).toDF()
    updatedSalariesDF.createOrReplaceTempView("UpdatedSalaries")



    //------------------------------------------------------------------------------------------------
    //After a while, we will have an updated document that has the latest salaries of the players, you are expected to build your spark application to
    // support taking these updates and merge the new values of these players salaries into our warehouse.
    // The new file will only have the players with the updated salaries only.
    // Player Salary

    println("====================================================================")
    println("Merge new salaries to the warehouse")
    println("====================================================================")

    // Add index now...
    fifaDF = addColumnIndex(fifaDF).withColumn("index", monotonically_increasing_id)

    updatedSalariesDF = updatedSalariesDF.toDF("Name","Salary")

    // Add index now...
    updatedSalariesDF = addColumnIndex(updatedSalariesDF).withColumn("index", monotonically_increasing_id)

    /**
      * Add Column Index to data-frame
      */
    def addColumnIndex(df: DataFrame) = {
      spark.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map {
          case (row, index) => Row.fromSeq(row.toSeq :+ index)
        },
        // Create schema for index column
        StructType(df.schema.fields :+ StructField("index", LongType, false)))
    }

    fifaDF = fifaDF.limit(6)
    fifaDF.show()
    fifaDF.drop("Salary")

    updatedSalariesDF = updatedSalariesDF.limit(9)
    updatedSalariesDF.show()

    val newDf = fifaDF.join(updatedSalariesDF, Seq("index", "Name"), "left_outer")
      .drop(fifaDF.col("Salary")).drop(fifaDF.col("index")).show()

  }

}