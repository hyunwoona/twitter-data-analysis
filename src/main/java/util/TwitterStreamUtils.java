package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/*
 * Author: Eric Na (hyunwoo.na@gmail.com), Date created: 3/21/16
 * Util methods to pass twitter login, to create a stream, and to start computation / wait for termination of the stream
*/

public class TwitterStreamUtils {
  // create and return Spark Streaming context,
  // which sends data at {streamBatchDurationInSec} second intervals in batches
  public static JavaStreamingContext getJavaStreamingContext(String appName, String twitterCredentialFilePath,
                                                              long streamBatchDurationInSec) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
    setTwitterConfig(twitterCredentialFilePath);
    JavaSparkContext sc = new JavaSparkContext(conf);
    return new JavaStreamingContext(sc, Seconds.apply(streamBatchDurationInSec));
  }

  // add Shutdown hook, start computation, and wait for termination of stream
  public static void startAndWaitTermination(final JavaStreamingContext jssc) {
    // handling the shut down gracefully
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down streaming app...");
        jssc.stop(true, true);
        System.out.println("Shutdown of streaming app complete.");
      }
    });
    jssc.start(); // Start the computation
    jssc.awaitTermination();
  }

  // Reads properties file, gets the credentials, and sets them as System properties.
  // @args configFilePath: full path to a textfile that contains a twitter login. See twitter4j.properties.template
  public static void setTwitterConfig(String configFilePath) {
    Properties properties = new Properties();
    InputStream configFile = null;

    try {
      configFile = new FileInputStream(configFilePath);
      properties.load(configFile);

      //pass Twitter credentials as System properties
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