package com.suning.spark.mlib
import org.apache.spark._
import org.apache.spark.sql.Row
import java.util.{ HashMap => JHashMap }
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.Set
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.regression.LabeledPoint
import scala.math._
import org.apache.log4j.Logger
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.{ LinearRegressionWithSGD, LassoWithSGD }
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.classification.SVMWithSGD


/**
 * @author 14070345
 */

object LogisticRegressionWithLBFGS {
  val dataSelect: String = "select * from spark.sfh_short_data_sample1 where decision_days > 0  and (pc_group_fourpage_pv +wap_group_fourpage_pv +app_group_fourpage_pv)/decision_days <=1 and l1_gds_group_cd='R8232' "
  val computeMetaDataSelect: String = "select * from spark.datavar"
  val dataTableStructSelect: String = "describe  spark.sfh_short_data_sample1"
  def is_odd(x: Double, compareValue: Double): Double = {   
    if (x > compareValue)
      compareValue
    else
      x
  }


 

  def main(args: Array[String]): Unit = {
    
    if(args.length<1){
      println("usage: *jar param")
    }
      
    val path:String = "/user/spark/model/LogisticRegressionWithLBFGS/"+args(0).toString()
    val logger = Logger.getLogger(LogisticRegressionWithLBFGS.getClass);
    val sparkConf = new SparkConf().setAppName("LogisticRegressionWithLBFGS")
    val sc = new SparkContext(sparkConf)
   

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val tableData = sqlContext.sql(dataSelect)

    val computeMetaData = sqlContext.sql(computeMetaDataSelect)

    val datatableStruct = sqlContext.sql(dataTableStructSelect)

    var i = 1
    import java.util.{ HashMap => JHashMap }

    val datatableStructMap = new JHashMap[Int, String]()
    tableData.columns.foreach { x =>

      datatableStructMap.put(i, x.toString())
      i = i + 1
    }

    /*
     * for j in range(5,67): 
     */
    val lableTableVectorDF = tableData.select("is_order", "pc_gds_addcart", "pc_gds_collect_num", "pc_gds_four_page_pv", "pc_gds_four_page_time",
      "pc_gds_four_page_fromsearch_pv", "pc_gds_four_page_fromlist_pv", "pc_gds_four_page_fromrec_pv",
      "pc_gds_four_page_fromcuxiao_pv", "pc_four_page_num", "pc_group_gds_addcart", "pc_group_gds_collect",
      "pc_group_fourpage_pv", "pc_group_fourpage_time", "pc_visitor_pv", "pc_search_pv", "pc_list_pv",
      "pc_is_view1", "pc_is_view", "pc_view_cycle_days", "pc_view_days", "wap_gds_addcart",
      "wap_gds_collect_num", "wap_gds_four_page_pv", "wap_gds_four_page_time", "wap_gds_four_page_fromsearch_pv",
      "wap_gds_four_page_fromlist_pv", "wap_gds_four_page_fromrec_pv", "wap_gds_four_page_fromcuxiao_pv",
      "wap_four_page_num", "wap_group_gds_addcart", "wap_group_gds_collect", "wap_group_fourpage_pv",
      "wap_group_fourpage_time", "wap_visitor_pv", "wap_search_pv", "wap_list_pv", "wap_is_view1",
      "wap_is_view", "wap_view_cycle_days", "wap_view_days", "app_gds_addcart", "app_gds_collect_num",
      "app_gds_four_page_pv", "app_gds_four_page_time", "app_gds_four_page_fromsearch_pv",
      "app_gds_four_page_fromlist_pv", "app_gds_four_page_fromrec_pv", "app_gds_four_page_fromcuxiao_pv",
      "app_four_page_num", "app_group_gds_addcart", "app_group_gds_collect", "app_group_fourpage_pv",
      "app_group_fourpage_time", "app_visitor_pv", "app_search_pv", "app_list_pv", "app_is_view1", "app_is_view",
      "app_view_cycle_days", "app_view_days", "gds_score_desc", "l4_gds_group_rate_n")
      
     
      lableTableVectorDF.limit(1000).rdd.saveAsTextFile(path+"/lableTableVectorDF")

    val computeMetaDataRDD = sqlContext.sql("select * from spark.datavar").rdd

    val metaArray = computeMetaDataRDD.collect().toArray

    def productLabeledPoint(t: Row): LabeledPoint = {
      var values = ArrayBuffer[Double]()

      for (i <- (1 to 62)) {
        val result = (t(i).toString().toDouble - (if (metaArray(i - 1).get(3) == null) { 0.0 } else { metaArray(i - 1).getDouble(3) })) / (if (metaArray(i - 1).get(4) == null) { 0.0 } else { metaArray(i - 1).getDouble(4) })
        values += is_odd(result, 3)
      }
      LabeledPoint(t(0).toString().toDouble, Vectors.dense(values.toArray))
    }

    //  t => productLabeledPoint(t)   => if code is complex ,mast use fuction instead of code block
    val lableTableVectorRDD = lableTableVectorDF.rdd.map {
      t => productLabeledPoint(t)
    }

  
    /**
     * splits data  and cache data
     */

    val partitionRDD = lableTableVectorRDD.coalesce(200, true).cache()
    val splits = partitionRDD.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()
    
    
    
 //  val algorithm = new LassoWithSGD()
    
/*     val algorithm = new LogisticRegressionWithSGD()
     algorithm.optimizer.setNumIterations(100000).setUpdater(new L1Updater())*/
    
    
    val algorithm=  new LogisticRegressionWithLBFGS()
    
    algorithm.optimizer.setRegParam(args(0).toDouble)
    
    
    algorithm.optimizer.setNumIterations(100000)

    

    
    algorithm.setIntercept(true)
    algorithm.setValidateData(true)
 
    val model = algorithm.run(partitionRDD)
    

    model.save(sc, path+"/model")
     model.clearThreshold()
     
    val modelDF=sqlContext.read.parquet(path+"/model/data/part-r-00001.gz.parquet")
    val common_metadata=sqlContext.read.parquet(path+"/model/data/_common_metadata")

    val metadata=sqlContext.read.parquet(path+"/model/data/_metadata")
    
    
    modelDF.rdd.saveAsTextFile(path+"/model/modeltxt/")
    common_metadata.rdd.saveAsTextFile(path+"/model/common_metadata/")
    metadata.rdd.saveAsTextFile(path+"/model/model/metadata/")
    
    val prediction = model.predict(partitionRDD.map(_.features))
    val predictionAndLabel = prediction.zip(partitionRDD.map(_.label))
    
    val predictiondata= prediction.zip(partitionRDD)
  
     prediction.coalesce(1, true).saveAsTextFile(path+"/prediction")
     predictionAndLabel.coalesce(1, true).saveAsTextFile(path+"/predictionAndLabel")
     predictiondata.coalesce(1, true).saveAsTextFile(path+"/predictiondata")
     
    import  org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    val metrics = new BinaryClassificationMetrics(predictionAndLabel)


    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")
    
 
    
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    
   
    val rmse = math.sqrt(loss / test.count())
    val areaUnderPR_areaUnderROC = sc.makeRDD(List("Test areaUnderPR = ${metrics.areaUnderPR()}:" + metrics.areaUnderPR(), 
                                                  "Test areaUnderROC = ${metrics.areaUnderROC()}:" + metrics.areaUnderROC()                                              
                                                    ), 
                                                   1)
                                               
    metrics.recallByThreshold().saveAsTextFile(path+"/recallByThreshold")  
    metrics.precisionByThreshold().saveAsTextFile(path+"/precisionByThreshold")  
    areaUnderPR_areaUnderROC.saveAsTextFile(path + "/areaUnderPR_areaUnderROC")
    sc.makeRDD(List(rmse), 1).saveAsTextFile(path + "/rmse")
    sc.stop()

  }

}
