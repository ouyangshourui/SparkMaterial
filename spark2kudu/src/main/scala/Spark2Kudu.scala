import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import com.google.common.collect.ImmutableList
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.{Schema, Type}
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import collection.JavaConverters._

object Spark2Kudu {
  case class Customer(name:String, age:Int, city:String)

  def main(args: Array[String]): Unit = {


    var sc: SparkContext = null
    var kuduClient: KuduClient = null
    var table: KuduTable = null
    var kuduContext: KuduContext = null

    val tableName = "spark-test"

    val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

    val conf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.app.id", appID)
    kuduClient = new KuduClientBuilder("172.172.241.228:7051").build()
    sc = new SparkContext(conf)
    kuduContext = new KuduContext("172.172.241.228:7051",sc)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.options(Map("kudu.master" -> "172.172.241.228:7051", "kudu.table" -> "app_event_aggre_mem_user_kudu_da"))
      .kudu.limit(2000)

    df.show(100)
     println("****************")
    df.schema.foreach(p=>println(p))


    if(kuduContext.tableExists("spark_test_table"))
      {
        kuduContext.deleteTable("spark_test_table")
      }
    kuduContext.createTable(
      "spark_test_table", df.schema, Seq("fuid"),
      new CreateTableOptions()
        .setNumReplicas(3)
        .addHashPartitions(List("fuid").asJava, 3))

    kuduContext.insertRows(df, "spark_test_table")

  }


}
