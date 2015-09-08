package com.suning.spark.webcapture
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * @author 14070345
 */
class TimeUt {

  def calcBeforeDate(day:Int):String={
    val now =Calendar.getInstance();
    now.setTime(new Date())
    now.set(Calendar.DATE, now.get(Calendar.DATE) - day + 1)
    val sdf = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy",Locale.ENGLISH); 
    sdf.format(now.getTime())   
  }
  
}

object TimeUt {
  val sdf = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy",Locale.ENGLISH);
    def calcBeforeDate(day:Int):String={
    val now =Calendar.getInstance()
    now.setTime(new Date())
    now.set(Calendar.DATE, now.get(Calendar.DATE) - day + 1)
    sdf.format(now.getTime())   
  }
     def calcBeforeDate(day:Int,date:Date):String={
    val now =Calendar.getInstance()
    now.setTime(new Date())
    now.set(Calendar.DATE, now.get(Calendar.DATE) - day)
    sdf.format(now.getTime())   
  }
  /*  def calcBeforeMin(min:Int):String={
    val now =Calendar.getInstance()
    now.setTime(new Date())
    now.set(Calendar.MINUTE, now.get(Calendar.MINUTE) - min + 1)
    sdf.format(now.getTime())   
  }*/
    
     def calcBeforeMin(min:Int,date:Date):Long={
     val now =Calendar.getInstance()
     now.setTime(date)
     now.getTimeInMillis-1000*min*60   
  }
     def calcBeforeMin(min:Int):Long={
     val now =Calendar.getInstance()
     now.setTime(new Date())
     now.getTimeInMillis-1000*min*60    
  }
    
    def calcgetMinill(date:String):Long={
    val now =Calendar.getInstance()
    val du = sdf.parse(date)
    du.getTime   
  }
        
}