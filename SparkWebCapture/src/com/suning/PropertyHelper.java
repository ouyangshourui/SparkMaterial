package com.suning;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;



public class PropertyHelper {
  private static String configPropertiesPath = "conf\\properties";
  public static Properties props = new Properties();
 
          
   
    /**
    * read value from file 
    * @param key
    *            
    * @return String
    */
    public static String getKeyValue(String key) {
        return props.getProperty(key);
    }
    /**
    * read value
    * @param filePath 
    * @param key 
    */ 
    public static String readValue(String filePath, String key) {
        Properties props = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(
                    filePath));
            props.load(in);
            String value = props.getProperty(key);
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /**
    * update or write properties
    * if exist update it
    * !exist  the write it
    */ 
    public static void writeProperties(String keyname,String keyvalue) {  

        try {
            OutputStream fos = new FileOutputStream(configPropertiesPath);
            props.setProperty(keyname, keyvalue);
            props.store(fos, "Update '" + keyname + "' value");
        } catch (IOException e) {
            System.err.println("update failed");
        }
    }
    /**
    * update properties
    * 
    */ 
    public static void updateProperties(String keyname,String keyvalue) {
        try {
            props.load(new FileInputStream(configPropertiesPath));
            OutputStream fos = new FileOutputStream(configPropertiesPath);            
            props.setProperty(keyname, keyvalue);
            props.store(fos, "Update '" + keyname + "' value");
        } catch (IOException e) {
            System.err.println("update failed");
        }
    }
    
    public static boolean setPropertiesPath(String filePath){
     // InputStream in=PropertyHelper.class.getClassLoader().getResourceAsStream(filePath);
    	InputStream in = null;
		try {
			in = new FileInputStream(filePath);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			 if(in == null){
			        System.err.println("the file not exists:"+filePath);
			        return false;
		      }		
		}
     
      
    //  LOG.info("setConfigPropertiesPath:"+filePath);
      try {
        PropertyHelper.props.load(in);
      } catch (IOException e) {
        
        e.printStackTrace();
      }
      return true;
    
    }
    
    public static String getPropertiesPath(){
   
      return configPropertiesPath;
      
    }

  public static void main(String[] args) {
		PropertyHelper.setPropertiesPath("conf//config.properties");
       System.out.println(PropertyHelper.getKeyValue("zookeeper.znode.parent"));
  }
}