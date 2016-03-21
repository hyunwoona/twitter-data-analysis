package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author: eric, Date created: 3/20/16
 */
public class TwitterConfigUtil {
  // Reads properties file, gets the credentials, and sets them.
  public static void setTwitterConfig(String configFilePath) {
    Properties properties = new Properties();
    InputStream configFile = null;

    try {
      configFile = new FileInputStream(configFilePath);
      properties.load(configFile);

      //pass Twitter credentials as System Properties
      System.setProperty("twitter4j.oauth.consumerKey", properties.getProperty("consumerKey"));
      System.setProperty("twitter4j.oauth.consumerSecret", properties.getProperty("consumerSecret"));
      System.setProperty("twitter4j.oauth.accessToken", properties.getProperty("accessToken"));
      System.setProperty("twitter4j.oauth.accessTokenSecret", properties.getProperty("accessTokenSecret"));

    } catch (IOException e) {
      System.err.println("Error while reading twitter config file: " + e);
    } finally {
      if (configFile != null) {
        try {
          configFile.close();
        } catch (IOException e) {
          System.err.println("Error while closing twitter config file: " + e);
        }
      }
    }
  }
}
