package com.suning.spark.mlib
import org.apache.spark._
import java.util.{HashMap => JHashMap}
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.Set
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import scala.math._

 import org.apache.spark.mllib.optimization.L1Updater
 import org.apache.spark.mllib.regression.{LinearRegressionWithSGD,LassoWithSGD}

/**
 * @author 14070345
 */


object Regression {
  val dataSelect:String="select * from spark.sfh_short_data_sample1 where decision_days > 0  and (pc_group_fourpage_pv +wap_group_fourpage_pv +app_group_fourpage_pv)/decision_days <=1 limit 10"
  val computeMetaDataSelect:String="select * from spark.datavar"
  val dataTableStructSelect:String="describe  spark.sfh_short_data_sample"
  //val vectorDataSelect:String=null;
  
   def getDataStruct(sqlContext:SQLContext){
 
  }
   def is_odd(x:Double,compareValue :Double): Double={
    
    if (x >compareValue )
      compareValue
    else
    x
}
  
   def main(args: Array[String]): Unit = {
     val sparkConf = new SparkConf().setAppName("Regression").setJars(Array("/home/spark/workspace/regression/spark-examples_2.jar"))
     val sc = new SparkContext(sparkConf)   
     val  sqlContext=new org.apache.spark.sql.hive.HiveContext(sc)
         
     val tableData=sqlContext.sql(dataSelect)
       
     val computeMetaData=sqlContext.sql(computeMetaDataSelect)
         
     val datatableStruct=sqlContext.sql(dataTableStructSelect)
     
     var i =1
     import java.util.{HashMap => JHashMap}
     
     val datatableStructMap = new JHashMap[Int,String]() 
      tableData.columns.foreach { x => 
                                   
                                   datatableStructMap.put(i, x.toString()) 
                                    i=i+1}  
     
     
     
     //  for j in coll(5,67):get datatableStructMap(5)  to datatableStructMap(57)
     
    
   //  val vectorTableData=tableData.select(array.toArray.toString())
     val lableTableVectorDF=tableData.select("is_order","pc_gds_four_page_fromcuxiao_pv","app_gds_four_page_time","app_view_cycle_days",
                                        "decision_cycle_days","pc_group_fourpage_pv","pc_group_fourpage_time",
                                        "wap_view_days","app_gds_four_page_fromcuxiao_pv","pc_gds_four_page_time",
                                        "l4_gds_group_rate_n","wap_is_view1","app_group_fourpage_time","app_search_pv",
                                        "wap_gds_addcart","app_is_view","app_gds_addcart","app_gds_four_page_fromrec_pv",
                                        "wap_group_gds_addcart","app_group_gds_addcart","app_is_view1","app_list_pv",
                                        "app_gds_four_page_fromsearch_pv","wap_gds_four_page_fromsearch_pv","wap_group_gds_collect",
                                        "gds_score_desc","app_group_gds_collect","wap_gds_four_page_time","wap_gds_four_page_pv",
                                        "pc_gds_four_page_fromsearch_pv","wap_visitor_pv","app_gds_four_page_pv","pc_is_view",
                                        "wap_group_fourpage_pv","wap_group_fourpage_time","wap_four_page_num",
                                        "app_group_fourpage_pv","app_gds_four_page_fromlist_pv","wap_search_pv",
                                        "pc_list_pv","pc_gds_collect_num","pc_gds_addcart","pc_view_days",
                                        "wap_gds_collect_num","pc_gds_four_page_pv","pc_gds_four_page_fromlist_pv",
                                        "app_visitor_pv","pc_is_view1","pc_visitor_pv","pc_view_cycle_days","wap_is_view",
                                        "pc_group_gds_addcart","wap_view_cycle_days","app_gds_collect_num",
                                        "wap_gds_four_page_fromrec_pv","app_view_days","wap_list_pv",
                                        "wap_gds_four_page_fromcuxiao_pv","app_four_page_num","pc_gds_four_page_fromrec_pv",
                                        "pc_group_gds_collect","pc_search_pv","wap_gds_four_page_fromlist_pv","pc_four_page_num")
                                        
                                        
                                        
           
            /*      normalization  , construct   LabeledPoint,
             *     data.iloc[:,j]=(data.iloc[:,j]- data_var.iloc[j-5,3])/data_var.iloc[j-5,4]
             *       data.iloc[:,j]=list(map(is_odd,data.iloc[:,j])) 
             *       t(0)  is lable                    
             */
           
                                        
           
                                              
                                     
   /*                                     
       val  lableTableVectorRDD = lableTableVectorDF.rdd.map ( t => {     
         var values= ArrayBuffer[String]()
         for(i <- (1 to 63)){
           
           
           val computeMetaData=sqlContext.sql("select * from spark.datavar where id=="+i)
           
          // 
           
          //  val results=computeMetaData.where(computeMetaData("id") >=i-1 and computeMetaData("id") <=i-1).collect()
           val results=computeMetaData.where(computeMetaData("id") >=i and computeMetaData("id") <=i).collect()
          //  val result=  (t(i).toString().toDouble - results(0).get(3).toString().toDouble )/results(0).get(4).toString().toDouble 
              
            values+="select * from spark.datavar where id=="+i
            //values +=  is_odd(result,3)
         }  
         values
      //  LabeledPoint(t(0).toString().toDouble, Vectors.dense(values.toArray))
         
       }     
       )
       */
       
                                        
        val computeMetaDataRDD=sqlContext.sql("select * from spark.datavar").rdd
        
         val metaArray=computeMetaDataRDD.collect()
      
        
      
        // avoid vaule is null 
  
         
     /*   val  lableTableVectorRDD = lableTableVectorDF.rdd.mapPartitions(p=>{     
          
          p.map { t =>{
              var  avg_data:Double=0.0;
              var std_data:Double=0.0
               var values= ArrayBuffer[Double]()
               for(i <- (1 to 63)){
                                     
                if(metaArray(i).get(3)==null){
                   avg_data=0.0
                }else{
                    avg_data= metaArray(i).getDouble(3)
                }
                
                 if(metaArray(i).get(4)==null){
                   std_data=0.0
                }else{
                    std_data= metaArray(i).getDouble(4)
                }
                 val result=  (t(i).toString().toDouble -avg_data )/std_data
                 
                 values +=  is_odd(Math.abs(result),3)
               }
              LabeledPoint(t(0).toString().toDouble, Vectors.dense(values.toArray))
            }
          }      
        }, true)*/
       
       
          val  lableTableVectorRDD = lableTableVectorDF.rdd.map {     
          t =>{
               var values= ArrayBuffer[Double]()
               for(i <- (1 to 63)){         
                 val result=  (t(i).toString().toDouble -(if(metaArray(i).get(3)==null){0.0}else{metaArray(i).getDouble(3)}) )/(if(metaArray(i).get(4)==null){0.0}else{metaArray(i).getDouble(4)})
                 
                 values +=  is_odd(Math.abs(result),3)
               }
              LabeledPoint(t(0).toString().toDouble, Vectors.dense(values.toArray))
            }
          }      
              
        lableTableVectorRDD.cache()
             
       
       
       
       /**
        * splits data  and cache data
        */
        val splits = lableTableVectorRDD.randomSplit(Array(0.8, 0.2))
       val training = splits(0).cache()
       val test = splits(1).cache()
       
       
       
       /*
        * 
        * NumIterations=1000000
        * 
        * 
        */
   
   
      /* val algorithm = new LinearRegressionWithSGD()
        algorithm.optimizer
               .setNumIterations(10)
               .setUpdater( new L1Updater())*/
  val algorithm = new      LassoWithSGD() 
  algorithm.optimizer.setNumIterations(1000000).setUpdater(new L1Updater())
             
               
   val model = algorithm.run(training)      
   
   
    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val loss = predictionAndLabel.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / test.count())
                         
   }
  
}
