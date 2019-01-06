import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SqlThings {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("WordCnt").enableHiveSupport().getOrCreate()
    case class Order(order_id:Int,order_date:Long,order_customer_id:Int,order_status:String)
    val orders = spark.read.parquet("orders_parquet_uncompress").as[Order]
    val order_map = orders.map(dt=>(dt.order_status,1)).toDF(Seq("status","val1"):_*)
    val order_reduced = order_map.groupBy("status").agg(sum(col("val1"))).agg(avg(col("val1"))).filter("sum(val1)>10000")
    val ordercnt = order_map.groupBy("status").count().filter("sum(val1)>10000").orderBy("sum(val1)")

    orders.sort()



    //Partitions and size
    orders.rdd.getNumPartitions
    orders.rdd.partitions.length
    orders.coalesce(5)
    orders.repartition(7)


  }
}