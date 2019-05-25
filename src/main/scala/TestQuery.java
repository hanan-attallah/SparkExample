import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class TestQuery {


    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.WARN);


        System.out.println("Report Batching process has started ...");

        // Start the Spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[8]")
                .appName("Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> oldDS = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).load("/Users/hatallah/IdeaProjects/HelloScala/src/main/resources/FifaData.csv");
        //fifaDF.createOrReplaceTempView("Fifa");


//        oldDS = oldDS.withColumn("id", callUDF("setMonth", col("id")));

        oldDS = oldDS.limit(10);

        oldDS.show();

        //read the new salary from new file:
//        Dataset<Row> newDS = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).load("/Users/hatallah/IdeaProjects/HelloScala/src/main/resources/UpdatedSalaries.csv");
        Dataset<Row> newDS = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).load("/Users/hatallah/IdeaProjects/HelloScala/src/main/resources/NewFifaData.csv");
        //newFifaDF.createOrReplaceTempView("newFifaDF");

        newDS = newDS.limit(10);
        newDS.show();







        //todo join
//        Dataset allDataset = oldDS.join(
//                newDS,
//                oldDS.col("Name").equalTo(newDS.col("Name")),"left_outer");
//
//
//
//        List<String> columns = Arrays.asList("Name", "Nationality", "`Fifa Score`", "Club", "Value","Salary");
//
//        List<String> joinColumns = Arrays.asList("Name","Nationality");
//        List<String> aggregatedColumns = Arrays.asList("Age");
//
//        Dataset<Row> allDataset = mergeJobSourceTable(sparkSession, newDS, oldDS, columns, aggregatedColumns, joinColumns);
////
//        allDataset.sort("name").show();







        List<String> columns = Arrays.asList("root_brand", "year", "month", "brand_id", "job_id", "source", "event_datetime", "job_referrer_id", "social_media_channel");

        List<String> joinColumns = Arrays.asList("brand_id", "job_id", "source", "event_datetime");

        List<String> aggregatedColumns = Arrays.asList("applications", "hires", "interviews", "offers", "onboards", "passes", "proceeds", "righttoworks", "views", "keesing",
                "talent_central", "screening_stage_2", "screening_stage_3", "post_applications", "post_proceeds", "post_interviews",
                "post_righttoworks", "post_offers", "post_onboards", "post_hires", "post_keesing", "post_talent_central",
                "post_screening_stage_2", "post_screening_stage_3");






        Dataset<Row> aggregatedDatasetNew = sparkSession.read().format("csv").option("header", true).option("inferSchema", true).load("/Users/hatallah/IdeaProjects/HelloScala/src/main/resources/JobSource.csv");


        aggregatedDatasetNew.printSchema();
        aggregatedDatasetNew.show();

        aggregatedDatasetNew = aggregatedDatasetNew
                .groupBy(JavaConverters.asScalaIteratorConverter(columns.stream().map(Column::new).collect(Collectors.toList()).iterator()).asScala().toSeq())
                .agg(sum(aggregatedColumns.get(0)).alias(aggregatedColumns.get(0)), JavaConverters.asScalaIteratorConverter(aggregatedColumns.subList(1,aggregatedColumns.size()).stream().map(c-> sum(c).alias(c))
                        .iterator()).asScala().toSeq());

        aggregatedDatasetNew.show();




    }





    /**
     * This method merge all the records of the dataset that are new, preserved, modified.
     * @param newDS
     * @param oldDS
     * @param columns
     * @param aggregatedColumns
     * @param joinColumns
     * @return
     */
    protected static Dataset<Row> mergeJobSourceTable(SparkSession sparkSession, Dataset<Row> newDS, Dataset<Row> oldDS, List<String> columns, List<String> aggregatedColumns, List<String> joinColumns) {
        System.out.println("Inside Function - merge ETLJobSourceTable");

        newDS.createOrReplaceTempView("new_ds");
        oldDS.createOrReplaceTempView("old_ds");

        //add columns to select stm
        String selectStmt = columns.stream().map(col -> "IF(new_ds." +
                joinColumns.get(0) + " IS NOT NULL , new_ds." + col + ", old_ds." +
                col + ") AS " + col).collect(Collectors.joining(",", "SELECT ", " "));

        if(aggregatedColumns != null) {
            //add aggregation columns to select stm - ex: coalesce(3,0) + coalesce(null,0) = 3
            selectStmt += aggregatedColumns.stream().map(col -> "( coalesce ( new_ds." +
                    col + " , 0 ) + coalesce ( old_ds." + col + " , 0 ) ) AS " + col
            ).collect(Collectors.joining(",", " , ", " "));
        }

        //add the join condition
        String joinStmt = joinColumns.stream().map(col -> "new_ds." + col + " == " + "old_ds." + col)
                .collect(Collectors.joining(" AND ", "FROM new_ds FULL JOIN old_ds ON ", ""));

        System.out.println("query::: " + selectStmt + joinStmt);

        Dataset<Row> dataSet = sparkSession.sql(selectStmt + joinStmt);

        sparkSession.catalog().dropTempView("new_ds");
        sparkSession.catalog().dropTempView("old_ds");

        return dataSet;
    }

}


//IF(new_ds.Name IS NOT NULL , new_ds.Name, old_ds.Name) AS Name,
