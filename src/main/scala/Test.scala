import org.apache.spark
import org.apache.spark.sql.SparkSession
import spark.sql.functions._
object Test {
  def main(args: Array[String]): Unit = {
    val spark= new SparkSession()
    val dat= spark.read.parquet("")
    dat.describe()

    val data = spark.read.parquet("nyse_parquet1/part-r-00000-305a612b-43d4-4b37-a013-c2ec84c38d42.gz.parquet")
    //Fetch data of schema
    //data.transform()
    val fieldTypesList = data.schema.map(struct => struct.dataType)
    //val test_udf = udf[String,Double]()
    //udf
  }
}
