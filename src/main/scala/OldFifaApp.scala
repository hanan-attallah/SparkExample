
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object OldFifaApp {

	def main(args: Array[String]) {

//		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("org").setLevel(Level.WARN)

		// $example on:init_session$
		val spark = SparkSession
			.builder()
			.master("local[8]")
			.appName("Spark SQL basic example")
			.config("spark.some.config.option", "some-value")
			.getOrCreate()


/*		val inputDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(getClass.getResource("/FifaData.csv").getPath).toDF()
		var url = ""
		val baseUrl = "http://localhost:8080/countries/search?"

		val nationalityDF = inputDF.select("Nationality").distinct.toDF("Nationality")

		val callRestApi: (String => String) = (nationality: String) => {

			println("--------------")
			val url = s"""${baseUrl}page=0&nameList=${nationality.mkString(",")}"""
			println("url:: " + url)
			null
		}

		val sqlfunc = udf(callRestApi)


		val add = (a:Int, b:Int) => a + b
		println(add(1, 2)) // Result is 3

		nationalityDF.withColumn("continent", sqlfunc(col("nationality")))*/

		val inputDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(getClass.getResource("/FifaData.csv").getPath).toDF()

		inputDF.createOrReplaceTempView("Fifa")

		spark.udf.register("getNationality", (input: String) => {
			(input.replaceAll("&", "and").replaceAll(" ", "%20").replaceAll("ã","a").
				replaceAll("é","e").replaceAll("í","i"))
		})




		// Create a parameter map to pass to the Rest-Data-Source. This contains the target URL for Rest service,
		// the input table name that has sets of parameter values to be passed to the REST service and the http method supported by REST service

		var nationalityDF = spark.sql("SELECT getNationality(Nationality) as Nationality FROM Fifa F  ORDER BY Nationality").distinct()



		var count = nationalityDF.count()

		if(nationalityDF.rdd.getNumPartitions > (count/10+1).toInt){
			nationalityDF = nationalityDF.repartition((count/10+1).toInt)
		}


		var url = ""
		var baseUrl = "http://localhost:8080/countries/search?"
		var page = 0
		val nameList = nationalityDF.collect().mkString(",")
		var i = 0;


		/*var newnationalityDF = nationalityDF.select("Nationality").limit(10).orderBy("Nationality")
		url = s"""${baseUrl}page=i&nameList=${nationalityDF.rdd.map(r => r(0)).collect().mkString(",")}"""
		println("url::::: " + url)
		val jsonString = scala.io.Source.fromURL(url)*/

		for( i <- 0 to (nationalityDF.rdd.partitions.size)) {
//			var newNationalityDF = nationalityDF.select("Nationality").limit(10).orderBy("Nationality")
			url = s"""${baseUrl}page=${i}&nameList=${nationalityDF.rdd.map(r => r(0)).collect().mkString(",")}"""
			println("url::::: " + url)
			val jsonString = scala.io.Source.fromURL(url)

		}
		//println( " :::::::::::::::  " + jsonString)

//		val inputDF2 = nationalityDF.createOrReplaceGlobalTempView("Nationality")


		//var parmg = Map("url" -> baseUrl, "input" -> "Nationality", "method" -> "GET", "partitions" -> "4", "schemaSamplePcnt" -> "10")

		// Now we create the Data-frame which contains the result from the call to the Soda API for the 3 different input data points
		//val sodasDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

		// We inspect the structure of the results returned. For Soda data source it would return the result in array.
		//sodasDf.printSchema


		import spark.implicits._ // spark is your SparkSession object
		import org.json4s._
		import org.json4s.jackson.JsonMethods._

//		val countryToContinent = scala.collection.mutable.Map[String, String]()
//		nationalityDF.foreach { case (nationality, idx) =>
//			val url = s"${baseUrl}page=0&nameList=${nationality.get(0)}"
//////			println(s"url: ${url}")
//			val jsonString = scala.io.Source.fromURL(url).mkString
////			println(nationality.getString(0) + " ::  " + jsonString + " size = " + jsonString.size)
////
////			if(jsonString.size>2){
//
//			val result = JSON.parseFull(jsonString)
//
//
//
//
//			result match {
//				// Matches if jsonStr is valid JSON and represents a Map of Strings to Any
//				case Some(map: Map[String, Any]) => countryToContinent += ("name" -> "continentName")
//				case None => println("Parsing failed")
//				case other => println("Unknown data structure: "+other)
//			}

//			val df = parse(scala.io.Source.fromURL(s"${baseUrl}page=0&nameList=${nationality.get(0)}").mkString)
//			val df = parse(jsonString).values.asInstanceOf[Map[String, Any]]
//			}


				//val a = (tuple + "").split(",")(0).replace("(", "")
				//val b = (tuple + "").split(",")(1).replace(")", "")
				//println(a, b, a)
				//countries+= new Tuple2(a, b)
				//countryToContinent += (jsonString -> b)





	}

	private def manageFifaData(spark: SparkSession): Unit = {
		/*


        val inputDF = spark.read.format("csv").option("header", true).option("inferSchema", true).load(getClass.getResource("/FifaData.csv").getPath)

        println("size: " + inputDF.rdd.partitions.size)

        // Assign the “table name” readings to the inputDS Dataset
        inputDF.createOrReplaceTempView("Fifa");


    */



		//var countries  = new MutableList[Tuple2[String, String]]( )
		/*val countryToContinent = scala.collection.mutable.Map[String, String]()

		do {
			url = baseUrl + "page=" + page + "&nameList="

			val jsonString = scala.io.Source.fromURL(url).mkString

			println(jsonString)

			val result = JSON.parseFull(jsonString).map {
				case json: Map[String, List[Map[String, Map[String, String]]]] =>
					json("content").map(l => (l("name"), l("continentCode")("name")))
			}.get

			result.foreach(tuple => {

				val a = (tuple + "").split(",")(0).replace("(", "")
				val b = (tuple + "").split(",")(1).replace(")", "")
				//println(a, b, a)
				//countries+= new Tuple2(a, b)
				countryToContinent += (a -> b)
			})
			rows = result.length
			page = page + 1

		} while (rows <= 10)
*/

		val ValueClean = {}


		spark.udf.register("getValue", (input: String) => {
			(input.replaceAll("€", "")).replaceAll("M", "")
		})


		spark.udf.register("getSalary", (input: String) => {
			(input.replaceAll("€", "")).replaceAll("K", "")
		})

		//spark.udf.register("getContinent", (input: String) => countryToContinent.get(input))




		val sqlDF1 = spark.sql("SELECT F.name as player, getContinent(Nationality) as continent FROM Fifa F  ORDER BY Nationality")
		sqlDF1.show()

		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlDF1.createOrReplaceTempView("player_continent")
		// Save the result in the output folder
		//1.Use the provided data sample to store a new file that contains the player name and the continent he is coming from. You are expected to use the API to do that.
		sqlDF1.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/player_continent")

		//2.Using parallelism is a requirement, We are expecting one file per continent as an output.
		sqlDF1.write.partitionBy("continent").format("csv").mode("overwrite").option("header", true).save("spark_files_output/player_continent_parts")

		//------------------------------------------------------------------------------------------------
		//sqlDF.write.mode("overwrite").saveAsTable("HiveResult55")
		//After a while, we will have an updated document that has the latest salaries of the players, you are expected to build your spark application to support taking these updates and merge the new values of these players salaries into our warehouse. The new file will only have the players with the updated salaries only.
		// Palyer Salary
		val sqlSalaryDF = spark.sql("SELECT name, Salary FROM Fifa F  ")
		sqlSalaryDF.show()
		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlSalaryDF.createOrReplaceTempView("player_salary")
		// Save the result in the output folder
		sqlSalaryDF.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/player_salary")

		//------------------------------------------------------------------------------------------------
		// Which top 3 countries that achieve the highest income through their players?
		val sqlTopCountries = spark.sql("SELECT Nationality, sum(getSalary(Salary)) as total FROM Fifa GROUP BY  Nationality ORDER BY total DESC limit 3")
		sqlTopCountries.show()
		// Assign the “table name”  result1  to the sqlDF Dataset
		sqlTopCountries.createOrReplaceTempView("top_3_countries_income")
		// Save the result in the output folder
		sqlTopCountries.write.format("csv").mode("overwrite").option("header", true).save("spark_files_output/top_3_countries_income")

		//------------------------------------------------------------------------------------------------
		//List The club that includes the most valuable players
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
		//here we assume America is 2 continents: North America, South America
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

		//------------------------------------------------------------------------------------------------
		//sqlDF.write.mode("overwrite").saveAsTable("HiveResult55")
		//After a while, we will have an updated document that has the latest salaries of the players, you are expected to build your spark application to support taking these updates and merge the new values of these players salaries into our warehouse. The new file will only have the players with the updated salaries only.
		// Save the result in the output folder
		spark.sql("DROP TABLE IF EXISTS player_salary") //spark 2.0
		sqlDF.write.format("csv").mode("overwrite").option("header", true).saveAsTable("player_salary")

		//------------------------------------------------------------------------------------------------
		// Which top 3 countries that achieve the highest income through their players?
		spark.sql("DROP TABLE IF EXISTS top_3_countries_income") //spark 2.0
		sqlDF.write.format("csv").mode("overwrite").option("header", true).saveAsTable("top_3_countries_income")
		//continent_best_score

		//------------------------------------------------------------------------------------------------
		//List The club that includes the most valuable players
		spark.sql("DROP TABLE IF EXISTS max_player_valuable_club") //spark 2.0
		sqlDF.write.format("csv").mode("overwrite").option("header", true).saveAsTable("max_player_valuable_club")

		//------------------------------------------------------------------------------------------------
		//The top 5 clubs that spends highest salaries
		spark.sql("DROP TABLE IF EXISTS top_5_player_valuable_clubs") //spark 2.0
		sqlDF.write.format("csv").mode("overwrite").option("header", true).saveAsTable("top_5_player_valuable_clubs")

		//------------------------------------------------------------------------------------------------
		//Which of Europe or America - on continent level - has the best FIFA players?
		//here we assume America is 2 continents: North America, South America
		spark.sql("DROP TABLE IF EXISTS continent_best_score") //spark 2.0
		sqlDF.write.format("csv").mode("overwrite").option("header", true).saveAsTable("continent_best_score")


	}

}