package com.suning.spark.webcapture

/**
 * @author 14070345
 */
object test {
  
  def main(args: Array[String]): Unit = {
     val address= "<a href=\"http://10.104.74.6:7081\">worker-20150805202205-10.104.74.6-8079</a>"
    
    address.split("\"").foreach { x => println(x)}
  
  }

 
  
}