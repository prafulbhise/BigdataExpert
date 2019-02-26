// Author: Praful Kumar Bhise
// Description: Sample programme to Load the data from ADLS RAW to ADLS CLEANSED
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import java.util._


object RawtoCleansed {
    def main(args: Array[String]) {
val conf = new SparkConf().setAppName("datalake raw to cleansed ingestion Application")
val sc = new SparkContext(conf)
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type","ClientCredential")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id","<id here>")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential","<put your credential here")
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url","https://login.windows.net/<access_key>/oauth2/token")
val date = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date())
val inputRawPath = args(0)
val read_inputRawPath = spark.read.option("delimiter","\u0007").format("com.databricks.spark.csv").load(inputRawPath)
val AppendDate_part = read_inputRawPath.withColumn("date_part",current_date())
val outPutCleansedPath = args(1)
AppendDate_part.write.format("com.databricks.spark.avro").partitionBy("date_part").mode("append").save(outPutCleansedPath) 
  }
}


spark-submit --packages com.databricks:spark-csv_2.10:1.4.0,com.databricks:spark-avro_2.10:2.0.1 --class "org.maersk.spark.RawtoCleansed" --master local[*] target/scala-2.10/rawtocleansed_2.10-1.0.jar "/datalake-poc/raw/app/data/team" "/datalake-poc/cleansed/app/data/team"