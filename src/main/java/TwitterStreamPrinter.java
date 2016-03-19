import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.auth.OAuth2Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import twitter4j.conf.ConfigurationBuilder;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

/**
 * Author: eric, Date created: 3/18/16
 * Creates a stream using Twitter Streaming API, gets Tweet Status, and prints the status text.
 * This is a starting point. More code will be added.
 */
public class TwitterStreamPrinter {
  //args[0]: full path to a textfile that contains a twitter login. See twitter4j.properties.template
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterStreamPrinter <Path to twitter credential file>");
      System.exit(1);
    }
    String twitterCredentialFilePath = args[0];
    setTwitterConfig(twitterCredentialFilePath);

    SparkConf conf = new SparkConf().setAppName("TwitterStreamPrinter").setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(conf);
//    final JavaStreamingContext jssc = new JavaStreamingContext(sc, Minutes.apply(5));
    final JavaStreamingContext jssc = new JavaStreamingContext(sc, Seconds.apply(20));

    JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

    //takes a Status object in Dstream, maps it to the text in the Status.
    //TODO: Collect Hashtags. Rank them in descending order of occurence.
    JavaDStream<String> statuses = twitterStream.map(new Function<Status, String>() {
      public String call(Status status) throws Exception {
        return status.getText();
      }
    });

    statuses.print(); //TODO: replace to writing to a text file

    // handling the shut down gracefully
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down streaming app...");
        jssc.stop(true, true);
        System.out.println("Shutdown of streaming app complete.");
      }
    });

    jssc.start();
    jssc.awaitTermination();
  }


  private static void setTwitterConfig(String configFilePath) {
    Properties properties = new Properties();
    InputStream configFile = null;

    try {
      configFile = new FileInputStream(configFilePath);
      properties.load(configFile);

      //set Twitter credentials on Spark Conf
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
