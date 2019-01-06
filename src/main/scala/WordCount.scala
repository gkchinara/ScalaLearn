import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCnt").enableHiveSupport().getOrCreate()
    val dt =  spark.read.text("File1.csv").toDF("line").as[String]
    dt.withColumn("count",size(split(col("line"),","))).select(sum(col("count"))).show()
    dt.flatMap{line => line.split(",")}.groupBy("value").count()

  }
}
