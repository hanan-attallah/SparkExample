import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JobSource {


    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[8]")
                .appName("Hanan")
//                .config("spark.eventLog.enabled", "true")
                .config("fs.s3n.awsAccessKeyId", "hanan")
                .config("fs.s3n.awsSecretAccessKey", "secret")
                .config("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
                .getOrCreate();

        Dataset<Row> jobSource = sparkSession
                .read()
                .format("orc")
                .option("header", "true")
                .load("s3n://dev-harri-reporting/dev_source_and_hire/job_source")
                .limit(20);

        jobSource.printSchema();

        jobSource.limit(20).write().format("csv")
                .mode("overwrite").option("header", true)
                .save("spark_files_output/jobSource");

    }


}
