
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import scala.util.parsing.json.JSON


object FifaApp {

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


		spark.udf.register("getNationality", (input: String) => {
			(input.replaceAll("&", "and").replaceAll(" ", "%20").replaceAll("ã", "a").
				replaceAll("é", "e").replaceAll("í", "i"))
		})

		val nationalityDF = spark.sql("SELECT getNationality(Nationality) as Nationality FROM Fifa F  ORDER BY Nationality").distinct().toDF()



		val baseUrl = "http://localhost:8080/countries/search?"
		var countryToContinent = Map[String, String]()

		println("nationalityDF.getNumPartitions::::::" + nationalityDF.rdd.getNumPartitions)

		import spark.implicits._
		val newDF = nationalityDF.mapPartitions((iterator) => {
			val myList = iterator.toList
			val length = myList.length / 10
			val nameList = myList.map(r => r(0)).mkString(",")
			if(myList.size>0) {
				for (i <- 0 to length) {
					val jsonString = scala.io.Source.fromURL(s"""${baseUrl}page=${i}&nameList=${nameList}""").mkString
					val newJsonString = s"""{"content": ${jsonString}}""".stripMargin
					JSON.parseFull(newJsonString).map {
						case json: Map[String, List[Map[String, Any]]] =>
							json("content").map(l => (countryToContinent += (l("name").toString -> l("continentName").toString)))
					}
				}
			}
			countryToContinent.toIterator
		}).toDF("Nationality", "continent")

		countryToContinent = newDF.map { r => (r.getString(0), r.getString(1)) }.collect.toMap


		// User Defined Function to get the values:
		spark.udf.register("getValue", (input: String) => {
			var output = 1.0
			if (input.contains("M")) {
				output = output * 10000
			}
			if (input.contains("k")) {
				output = output * 1000
			}
			val value = ((input.replaceAll("€", "")).replaceAll("M", "")).replaceAll("K", "")
			output = output * value.toDouble
			output
		})

		// User Defined Function to get the Salary:
		spark.udf.register("getSalary", (input: String) => {
			var output = 1.0
			if (input.contains("M")) {
				output = output * 1000000
			}
			if (input.contains("k")) {
				output = output * 1000
			}
			val value = ((input.replaceAll("€", "")).replaceAll("M", "")).replaceAll("K", "")
			output = output * value.toDouble
			output
		})

		spark.udf.register("getContinent", (input: String) => countryToContinent.get(input))


		//use sql
//		val sqlDF1 = spark.sql("SELECT F.name as player, getContinent(Nationality) as continent FROM Fifa F  ORDER BY Nationality")

		// 2nd solution , append new column
		fifaDF = fifaDF.withColumn("continent", expr("getContinent(Nationality)"))
		fifaDF.show()
		val sqlDF1 = fifaDF.select("name" , "continent").orderBy("Nationality")
		sqlDF1.show()

		//------------------------------------------------------------------------------------------------

		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlDF1.createOrReplaceTempView("player_continent")

		//------------------------------------------------------------------------------------------------


		//1.Use the provided data sample to store a new file that contains the player name and the continent he is coming from. You are expected to use the API to do that.
		sqlDF1.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/player_continent")

		//------------------------------------------------------------------------------------------------


		//2.Using parallelism is a requirement, We are expecting one file per continent as an output.
		sqlDF1.write.partitionBy("continent").format("csv").mode("overwrite").option("header", true).save("spark_files_output/player_continent_parts")



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

		val newDf = fifaDF.join(updatedSalariesDF, Seq("index", "Name"), "inner")
			.drop(fifaDF.col("Salary")).drop(fifaDF.col("index"))
			.repartition(1)
			.write
			.mode("overwrite")
			.option("header", true)
			.format("csv")
			.save("spark_files_output/FifaData")


		//------------------------------------------------------------------------------------------------
		// Which top 3 countries that achieve the highest income through their players?
		val sqlTopCountries = spark.sql("SELECT Nationality, sum(getSalary(Salary)) as total FROM Fifa GROUP BY  Nationality ORDER BY total DESC limit 3")

		val test = fifaDF.select(
			col("Nationality"),
			col("sum(getSalary(Salary))").as("total")
		).groupBy("Nationality")

		sqlTopCountries.show()
		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlTopCountries.createOrReplaceTempView("top_3_countries_income")
		// Save the result in the output folder
		sqlTopCountries.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/top_3_countries_income")

		//------------------------------------------------------------------------------------------------
		//List The club that includes the most valuable players
		println("List The club that includes the most valuable players:::")
		val sqlTopClub = spark.sql("SELECT Club, sum(getValue(Value)) as total FROM Fifa GROUP BY Club ORDER BY total DESC limit 1")
		sqlTopClub.show()
		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlTopClub.createOrReplaceTempView("max_player_valuable_club")
		// Save the result in the output folder
		sqlTopClub.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/max_player_valuable_club")

		//------------------------------------------------------------------------------------------------
		//The top 5 clubs that spends highest salaries
		val sqlTop5Club = spark.sql("SELECT Club, sum(getSalary(Salary)) as Salaries FROM Fifa GROUP BY Club ORDER BY Salaries DESC limit 5")
		sqlTop5Club.show()
		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlTop5Club.createOrReplaceTempView("top_5_player_valuable_clubs")
		// Save the result in the output folder
		sqlTop5Club.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/top_5_player_valuable_clubs")


		//------------------------------------------------------------------------------------------------
		//Which of Europe or America - on continent level - has the best FIFA players?
		//we assume America is 2 continents: North America, South America
		val sqlDF = spark.sql(
			"""
					SELECT getContinent(Nationality) as continent, avg(F.`Fifa Score`) as Score FROM Fifa F  WHERE getContinent(Nationality)  IN ( 'Europe' , 'North America', 'South America')
					GROUP BY continent
					ORDER BY Score DESC LIMIT 1""")
		sqlDF.show()
		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlDF.createOrReplaceTempView("continent_best_score")
		// Save the result in the output folder
		sqlDF.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/continent_best_score")



	}

}