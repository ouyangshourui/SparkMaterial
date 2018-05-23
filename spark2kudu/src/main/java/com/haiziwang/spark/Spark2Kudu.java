package com.haiziwang.spark;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.*;

import scala.collection.Seq;


public class Spark2Kudu {
    public static void main(String[] args) {
        SparkContext sc  = null;

        KuduTable table = null;
        KuduContext kuduContext  = null;
        String master = "172.172.241.228:7051";
        String tableName = "spark-test-java";
        String appID = "Spark2Kudu"+System.currentTimeMillis();
        SparkConf conf = new SparkConf().
                setMaster("local[*]").
                setAppName("test").
                set("spark.app.id", appID);
        sc = new SparkContext(conf);
        kuduContext = new KuduContext(master,sc);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        Map confmap= new HashMap<String, String>();
        confmap.put("kudu.master", master);
        confmap.put("kudu.table", "app_event_aggre_mem_user_kudu_da");
        KuduClient kuduClient = new KuduClient.KuduClientBuilder("172.172.241.228:7051").build();

        DataFrame df = sqlContext.read().format("org.apache.kudu.spark.kudu").options(confmap).load().limit(1000)
                .select("fuid","femail");
        df.show(100);
        System.out.println(df.schema());
        if(kuduContext.tableExists(tableName))
        {
            kuduContext.deleteTable(tableName);
        }

        ArrayList list = new ArrayList<String>();
        list.add("fuid");
        List<ColumnSchema> columns = new ArrayList(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("fuid", Type.INT64)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("femail", Type.STRING)
                .nullable(true)
                .build());
        Schema schema = new Schema(columns);
        try {
            kuduClient.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(list));

        } catch (KuduException e) {
            System.out.println("create kudu failed");
            e.printStackTrace();
        }

        kuduContext.insertRows(df, tableName) ;


    }
}
