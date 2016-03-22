package sentiment;

import scala.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Status;
import util.TwitterConfigUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


import com.google.common.io.Files;

/**
 * Author: Eric Na (hyunwoo.na@gmail.com), Date created: 3/20/16
 * Creates a stream using Twitter Streaming API, calculates the sentiment score of each tweet,
 * and writes the sentiment score and geo-location of the author to a file.
 */
public class TwitterSentimentAnalyzer {

  private static SentiWordNet sentiWordNet = new SentiWordNet();
  private static final String englishIsoLanguageCode = "en";
  private static int streamBatchDurationInSec = 600; //receive data at 600 second intervals in batches

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    if (args.length < 2) {
      System.err.println("Usage: TwitterSentimentAnalyzer <Path to twitter credential file> <Output file path>");
      System.exit(1);
    }

    String twitterCredentialFilePath = args[0];
    TwitterConfigUtil.setTwitterConfig(twitterCredentialFilePath);
    String outputFilePath = args[1];

    SparkConf conf = new SparkConf().setAppName("TwitterSentimentAnalyzer").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    final JavaStreamingContext jssc = new JavaStreamingContext(sc, Seconds.apply(streamBatchDurationInSec));

    JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

    File file = new File(outputFilePath);

    JavaPairDStream<Double, GeoLocation> sentimentScoreAndGeoLocations = getSentimentScoreAndGeoLocations(twitterStream);

    sentimentScoreAndGeoLocations.foreachRDD(rdd -> {
      writeScoreAndGeoLocationToFile(file, rdd);
    });

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

  //Get score and write to file
  public static JavaPairDStream<Double, GeoLocation> getSentimentScoreAndGeoLocations(
      JavaReceiverInputDStream<Status> twitterStream) {
    JavaDStream<Status> englishTweetsInUS = getTweetsWrittenInEnglish(twitterStream);
    JavaPairDStream<List<String>, GeoLocation> wordsAndGeoLocationInTweets = getWordsAndGeoLocation(englishTweetsInUS);
    return getScoreAndGeoLocation(wordsAndGeoLocationInTweets);
  }

  //calculate sentiment score from the lists of words, and map to a sentiment score and geo-location.
  private static JavaPairDStream<Double, GeoLocation> getScoreAndGeoLocation(JavaPairDStream<List<String>, GeoLocation> wordsAndGeoLocationInTweets) {
    return wordsAndGeoLocationInTweets
        .mapToPair(wordsAndGeoLocation ->
            new Tuple2<>(getSentimentScore(wordsAndGeoLocation._1), wordsAndGeoLocation._2));
  }

  //Filter out tweets that are not written in English, and tweets not posted in US
  private static JavaDStream<Status> getTweetsWrittenInEnglish(JavaReceiverInputDStream<Status> twitterStream) {
    return twitterStream
        .filter(status -> status.getGeoLocation() != null)
        .filter(status -> isRoughlyWithinUSAMainland(status.getGeoLocation()))
        .filter(status -> status.getLang().equals(englishIsoLanguageCode));
  }

  //map to lists of all words and the geo-location of author from each tweet.
  private static JavaPairDStream<List<String>, GeoLocation> getWordsAndGeoLocation(JavaDStream<Status> tweets) {
    return tweets
        .mapToPair(status -> new Tuple2<>(getAsWordsList(status.getText()), status.getGeoLocation()));
  }

  //write the result to database or file
  private static void writeScoreAndGeoLocationToFile(File file, JavaPairRDD<Double, GeoLocation> rdd) {
    rdd.foreach(tuple -> {
      try {
        // write score and location to the end of file
        Files.append(tuple._1 + "," + tuple._2.getLatitude() + "," + tuple._2.getLongitude() + "\n",
            file, Charset.forName("UTF-8"));
      } catch (IOException e) {
        System.err.println("error while writing output to file" + e);
      }
    });
  }

  // Applying a number of Regex, get only the words in a tweet text as a list
  private static List<String> getAsWordsList(String text) {
    final String urlRegex = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    final String nonWordRegex = "@\\w*|#\\w*|\\bRT\\b|[^@#\\p{L}\\p{N} ]+";
    final String contractionRegex = "\'s|\'m|\'re|\'ve";

    return Arrays.asList(
        text.replaceAll(contractionRegex, "")
            .replaceAll(urlRegex, "")
            .replaceAll(nonWordRegex, "")
            .trim().split("[ \n\t]"));
  }

  // Very roughly determines whether a geo-location falls within USA mainland boundary.
  // More accurate filtering will be applied on the client code.
  private static boolean isRoughlyWithinUSAMainland(GeoLocation geoLocation) {
    if (geoLocation == null) {
      return false;
    }
    final double westBoundingCoordinate = -125.0;
    final double eastBoundingCoordinate = -67.0;
    final double northBoundingCoordinate = 49.0;
    final double southBoundingCoordinate = 25.5;

    double latitude = geoLocation.getLatitude();
    double longitude = geoLocation.getLongitude();

    return southBoundingCoordinate <= latitude && latitude <= northBoundingCoordinate &&
        westBoundingCoordinate <= longitude && longitude <= eastBoundingCoordinate;
  }

  //Calculate sentiment score of a list of words in a text, using sentiWordNet.
  private static double getSentimentScore(List<String> words) {
    if (words.isEmpty()) {
      return 0;
    }
    double score = 0d;
    for (String word : words) {
      score += sentiWordNet.extract(word);
    }
    return score / words.size();
  }
}
