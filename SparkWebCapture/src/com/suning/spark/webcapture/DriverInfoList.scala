package com.suning.spark.webcapture
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import com.suning.alarm.client
import com.suning.PropertyHelper

/**
 * @author 14070345
 * 
 */
class driveInfo{
  var drivername:String=null
  var starttime:String=null
  var driverlocation:String=null
  var driverstate:String=null
  var drivercores:String=null
  var drivermemory:String=null
  var mainclass:String=null
  var appid:String=null
}



object DriverInfoList {
  def getState(doc:Document):(String,String) ={
     val state= doc.getElementsByTag("li").last()
     (state.toString().substring(12, 18),state.toString().substring(state.toString().length()-10, state.toString().length()-5))
  }
  
  def getActiveMaster(doc1:Document,doc2:Document):Document={
    
    if(getState(doc1)._2 =="ALIVE"){
      doc1
    }else{
      doc2
    }
              
  }
  
  def getCompletedApplicationsTable(masterdoc:Document):List[driveInfo]={
    val span12= masterdoc.getElementsByTag("div")
    val competedDriverspan = span12.get(span12.size-1).getElementsByTag("tr")   
    var i = 1
    var list = List[driveInfo]()
    while( i < competedDriverspan.size()){ 
       /*println()
       println("***************************************")
       println()*/
       val tables= competedDriverspan.get(i).getElementsByTag("td")
       var j = 0;
       var drivername:String=null
         var starttime:String=null
         var driverlocation:String=null
         var driverstate:String=null
         var drivercores:String=null
         var drivermemory:String=null
         var mainclass:String =null
        while(j<tables.size()){
         
          if(j ==0){
              drivername= tables.get(j).toString().substring("<td>".length(), tables.get(j).toString().length()-("/<td>".length()))
            // print(drivername)
          }
         if(j ==1){
             starttime=tables.get(j).toString().substring("<td>".length(), tables.get(j).toString().length()-("/<td>".length()))
          //   print(starttime)
          }
         if(j ==2){
             driverlocation=tables.get(j).toString().substring("<td>".length(), tables.get(j).toString().length()-("/<td>".length()))
          //  println(driverlocation)
          }
         if(j ==3){
              driverstate = tables.get(j).toString().substring("<td>".length(), tables.get(j).toString().length()-("/<td>".length()))
          //   print(driverstate)
          }
         if(j ==4){   
             val headlength = "<td sorttable_customkey=\"2\">".length()
              drivercores=tables.get(j).toString().substring(headlength, tables.get(j).toString().length()-("/<td>".length()))
           //  print(drivercores)
          }
          if(j ==5){
             val headlength = "<td sorttable_customkey=\"4096\">".length()
              drivermemory=tables.get(j).toString().substring(headlength, tables.get(j).toString().length()-("/<td>".length()))
            // print(drivermemory)
          }
          if(j ==6){
             val headlength = "<td sorttable_customkey=\"4096\">".length()
              mainclass=tables.get(j).toString().substring("<td>".length(), tables.get(j).toString().length()-("/<td>".length()))
           //  print(mainclass)

          }
            j  = j+1          
                      
        }
           var di=new driveInfo()
            di.drivername=drivername
            di.starttime=starttime
            di.driverlocation=driverlocation
            di.driverstate=driverstate
            di.drivercores=drivercores
            di.drivermemory=drivermemory
            di.mainclass=mainclass         
            list =di::list
         i  = i+1
    }
    
  //  println("\n\n\n*****************list driver info**********************")
   // list.reverse.foreach { x => println(x.starttime+":"+x.drivername+":"+x.driverstate)}
       
    list.reverse
  }
  
 
  def filterList(list:List[driveInfo],min:Int):List[driveInfo]={

    /*  for(  l <-compeleteddriverinfo) {
     
    if((TimeUt.calcgetMinill(l.starttime)-(now.getTime-1000*120*60))>0){
     println(TimeUt.calcgetMinill(l.starttime)+" ***** "+now.getTime) 
    }
 }*/
  //  val newlist = list.filter { x => (TimeUt.calcgetMinill(x.starttime)-(now.getTime-1000*min*60))>0 }   
   // newlist.foreach { x => println(x.drivername+"|"+x.starttime) }
    list.filter { x => (TimeUt.calcgetMinill(x.starttime)-((new java.util.Date()).getTime-1000*min*60))>0 }    
  }
  
  def dealfailedriver(driveinfo: driveInfo){
      
      val message="DriverID:"+driveinfo.drivername+"\n  driverstate:"+driveinfo.driverstate+"\n starttime: "+driveinfo.starttime
      com.suning.alarm.client.AlarmApp.alarm("spark生产环境", 1, message); 
      println(driveinfo.drivername+" "+driveinfo.driverstate+" "+driveinfo.starttime+" "+driveinfo.driverlocation)
          
  }
   def dealErrordriver(driveinfo: driveInfo){
     
    val message="DriverID:"+driveinfo.drivername+"\n  driverstate:"+driveinfo.driverstate+"\n starttime: "+driveinfo.starttime
    com.suning.alarm.client.AlarmApp.alarm("spark生产环境", 1, message); 
    println(driveinfo.drivername+" "+driveinfo.driverstate+" "+driveinfo.starttime+" "+driveinfo.driverlocation)      
    
  }
   
  def dealKILLEDdriver(driveinfo: driveInfo){   
      val message="DriverID:"+driveinfo.drivername+"\n  driverstate:"+driveinfo.driverstate+"\n starttime: "+driveinfo.starttime
      com.suning.alarm.client.AlarmApp.alarm("spark生产环境", 1, message); 
      println(driveinfo.drivername+" "+driveinfo.driverstate+" "+driveinfo.starttime+" "+driveinfo.driverlocation)         
  }
   def dealfinishdriver(driveinfo: driveInfo){  
     val appid= DriverState.getAppId(driveinfo)
     println("appid:"+appid)
     driveinfo.appid=appid
      println(driveinfo.drivername+" "+appid+" "+driveinfo.driverstate+" "+driveinfo.starttime+" "+driveinfo.driverlocation)         
  }
  
  def driverStateMatch(driveinfo: driveInfo) {
    driveinfo.driverstate match {
      case "FAILED" => dealfailedriver(driveinfo)
      case "FINISHED" =>dealfinishdriver(driveinfo)
      case "ERROR"  => dealErrordriver(driveinfo)
      case "UNKNOWN"  => println(driveinfo.drivername+" "+driveinfo.driverstate+" "+driveinfo.starttime+" "+driveinfo.driverlocation)
      case "KILLED"  => dealKILLEDdriver(driveinfo)
      case "RELAUNCHING"  => println(driveinfo.drivername+" "+driveinfo.driverstate+" "+driveinfo.starttime+" "+driveinfo.driverlocation)
      case _  => println(driveinfo.drivername+" "+driveinfo.driverstate +"is no DriverState")
    }

  }
  
  def main(args: Array[String]): Unit = {
    if(args.length !=1){
      println("useage:scala -c jar1:jar2:java3 classname config_properties_path")
     System.exit(-1)
    }
    PropertyHelper.setPropertiesPath(args(0));
    val master=PropertyHelper.getKeyValue("spark.master1")
    val master1 = Jsoup.connect(PropertyHelper.getKeyValue("spark.master1")).get()
    val master2 = Jsoup.connect(PropertyHelper.getKeyValue("spark.master2")).get() 
    val masterdoc = getActiveMaster(master1,master2)
    println("Active master:"+masterdoc.title())
  
    val compeleteddriverinfo=getCompletedApplicationsTable(masterdoc)
     
    filterList(compeleteddriverinfo,PropertyHelper.getKeyValue("spark.monitor.interval").toInt).foreach(x=>driverStateMatch(x))
            
  }

   
}