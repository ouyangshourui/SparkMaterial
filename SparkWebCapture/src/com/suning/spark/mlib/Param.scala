package com.suning.spark.mlib

import org.apache.spark.sql.SQLContext
import java.util.{HashMap => JHashMap}



/**
 * @author 14070345
 */
trait param {
  val dataSelect:String="select * from spark.sfh_short_data_sample where decision_days > 0  and (pc_group_fourpage_pv +wap_group_fourpage_pv +app_group_fourpage_pv)/decision_days <=1"
  val computeMetaDataSelect:String="select * from spark.datavar"
  val dataTableStructSelect:String="describe  spark.sfh_short_data_sample"
  //val vectorDataSelect:String=null;
  
   def getDataStruct(sqlContext:SQLContext){
 
  }
   def is_odd(n:Int): Int={
    
    if (n >3 )
      3
    else
    n
} 
}