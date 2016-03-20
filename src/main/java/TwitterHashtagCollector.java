import scala.Tuple2;
import twitter4j.Status;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.io.Files;

/**
 * Author: Eric Na (hyunwoo.na@gmail.com), Date created: 3/18/16
 * Creates a stream using Twitter Streaming API, gets top hashtags and tweets, and write to a file.
 */
public class TwitterHashtagCollector {
  private static final DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
  private static final String timeZoneId = "America/Los_Angeles";
  private static int streamBatchDurationInSec = 5;
  private static int windowDurationInSec = 300;

  //args[0]: full path to a textfile that contains a twitter login. See twitter4j.properties.template
  //args[1]: full path to a textfile to write hashtags and tweets to
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: TwitterHashtagCollector <Path to twitter credential file> <Output file path>");
      System.exit(1);
    }
    String twitterCredentialFilePath = args[0];
    setTwitterConfig(twitterCredentialFilePath);
    String outputFilePath = args[1];

    SparkConf conf = new SparkConf().setAppName("TwitterHashtagCollector").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    final JavaStreamingContext jssc = new JavaStreamingContext(sc, Seconds.apply(streamBatchDurationInSec));

    JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

    File file = new File(outputFilePath);

    //write top 25 hashtags and top 3 tweets to {file}
    writeTopHashtagsToFile(file, twitterStream, 25);
    writeMostRetweetedTweetsToFile(file, twitterStream, 3);

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

  //Takes a Twitter stream, gets the {numTopHashtags} hashtags with the highest counts, and write to {file}
  private static void writeTopHashtagsToFile(File file, JavaReceiverInputDStream<Status> twitterStream,
                                             int numTopHashtags) {
    DateTime currentTime = DateTime.now(DateTimeZone.forID(timeZoneId));

    JavaDStream<String> statusTextsWithHashtag = twitterStream
        .map(Status::getText)
        .filter(statusText -> statusText.contains("#")); //filter out tweets without hashtag

    JavaDStream<String> hashtags = statusTextsWithHashtag.flatMap((FlatMapFunction<String, String>) statusText -> {
      List<String> words = Arrays.asList(statusText.split("[ .,:\n\t]"));

      return words.stream()
          .filter(hashtagWord -> hashtagWord.startsWith("#")) //filter non-hashtag words
          .map(hashtagWord -> hashtagWord.replace("#", "")) //remove '#' character from hashtag words
          .collect(Collectors.toList());
    });

    JavaPairDStream<Long, String> countAndHashtagDescendingOrdered = hashtags
        .countByValue() //reduce RDDs of hashtags in DStream to PairRDDs of (hashtag, count)s
        .reduceByKeyAndWindow((count1, count2) ->
            count1 + count2, Seconds.apply(windowDurationInSec)) //further reduce by hashtag to get counts across RDDs
        .mapToPair(hashtagAndcount -> new Tuple2<>(hashtagAndcount._2, hashtagAndcount._1)) //swap order in tuple
        .transformToPair(rdd -> rdd.sortByKey(false)); //sort in descending order

    countAndHashtagDescendingOrdered.foreachRDD(rdd -> {
      String timeRange = dateTimeFormat.print(currentTime.minusSeconds(windowDurationInSec))
          + " to " + dateTimeFormat.print(currentTime) + " (Pacific Time)";
      Files.append("Top " + numTopHashtags + " hashtags captured from " + timeRange + ":\n",
          file, Charset.forName("UTF-8"));

      // Get top {numTopHashtags} hashtags as (count, hashtag) tuple.
      List<Tuple2<Long, String>> topHashtags = rdd.take(numTopHashtags);
      topHashtags.forEach(countAndHashtag -> {
        try {
          // write count and hashtag to the end of file
          Files.append(countAndHashtag.toString() + "\n", file, Charset.forName("UTF-8"));
        } catch (IOException e) {
          System.err.println("error while writing output to file");
        }
      });
    });
  }

  //Takes a Twitter stream, gets the {numTopTweets} hashtags with the highest counts, and write to {file}
  private static void writeMostRetweetedTweetsToFile(File file, JavaReceiverInputDStream<Status> twitterStream,
                                                     int numTopTweets) {
    JavaPairDStream<Integer, String> retweetCountAndTextDescendingOrdered = twitterStream
        .filter(Status::isRetweet) //only retweeted tweets are considered
        .mapToPair(status ->
            new Tuple2<>(status.getRetweetedStatus().getRetweetCount(),
                status.getText().replace("\n", " "))) //map to (retweet count, tweet text) pair. Remove newline from text
        .window(Minutes.apply(windowDurationInSec)) //capture the stream in the specified time window
        .transformToPair(rdd -> rdd.distinct()) //remove duplicates
        .transformToPair(rdd -> rdd.sortByKey(false)); //sort from the most retweeted tweets to the least retweeted ones

    retweetCountAndTextDescendingOrdered.foreachRDD(rdd -> {
      Files.append(numTopTweets + " most retweeted Tweets in this time window:\n",
          file, Charset.forName("UTF-8"));

      // Get top {numTopTweets} tweets as (retweet count, tweet text) tuple.
      List<Tuple2<Integer, String>> topTweets = rdd.take(numTopTweets);
      topTweets.forEach(countAndTweet -> {
        try {
          //write the tweet count and text to the end of file
          Files.append(countAndTweet._2 + " (retweeted " + countAndTweet._1 + " times)\n",
              file, Charset.forName("UTF-8"));
        } catch (IOException e) {
          System.err.println("error while writing output to file");
        }
      });
      Files.append("----------------------------------------------\n",
          file, Charset.forName("UTF-8"));
    });
  }

  // Reads properties file, gets the credentials, and sets them.
  private static void setTwitterConfig(String configFilePath) {
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
