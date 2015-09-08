package com.suning.spark.webcapture
import scala.io._
import java.net.{URL, URLEncoder}
/**
 * @author 14070345
 */
private[webcapture] object DriverState extends Enumeration {

  type DriverState = Value

  // SUBMITTED: Submitted but not yet scheduled on a worker
  // RUNNING: Has been allocated to a worker to run
  // FINISHED: Previously ran and exited cleanly
  // RELAUNCHING: Exited non-zero or due to worker failure, but has not yet started running again
  // UNKNOWN: The state of the driver is temporarily not known due to master failure recovery
  // KILLED: A user manually killed this driver
  // FAILED: The driver exited non-zero and was not supervised
  // ERROR: Unable to run or restart due to an unrecoverable error (e.g. missing jar file)
  val SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR = Value

  def getAppId(driveinfo: driveInfo):String={
    val splits= driveinfo.driverlocation.split("\"")
    val driveradress=splits(1)  
    val stderr1=driveradress+"/logPage/?driverId="+driveinfo.drivername.substring(0, driveinfo.drivername.length()-1)
    val stderr=stderr1.concat("&logType=stderr")
    val stderroffset=stderr1.concat("&logType=stderr&offset=0&byteLength=102400")
    import java.net.{URL, URLEncoder}
    var it:Iterator[String]=null;
    try{
      it= scala.io.Source.fromURL(stderr).getLines().filter { x => {
      x.contains("Connected to Spark cluster with app ID app-")
    } }
    if(it.isEmpty){
         it= scala.io.Source.fromURL(stderroffset).getLines().filter { x => {
      x.contains("Connected to Spark cluster with app ID app-")
      } }
    }   
    }catch{
      case ex:java.net.ConnectException => println(ex.toString())
    }
       
    if(it.isEmpty){
      null
    }else{
        
    var idline:String="";
    it.foreach { x => idline=idline+x }
    //  println(idline)
     val idlines=idline.split("ID")     
     idlines(1).toString()
    }
  }
  
  

}