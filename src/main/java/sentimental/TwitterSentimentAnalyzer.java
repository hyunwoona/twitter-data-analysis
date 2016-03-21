package sentimental;

import scala.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Status;
import util.TwitterConfigUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * Author: eric, Date created: 3/20/16
 */
public class TwitterSentimentAnalyzer {
  private static SentiWordNet sentiWordNet = new SentiWordNet();
  private static final String englishIsoLanguageCode = "en";
  //TODO: need to use a file instead of a DB, due to memory limit on free-tier Amazon EC2 machine. (can't run Spark there)
  private static final Connection conn = getDBConnection("localhost", "spark", "yewno");

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: TwitterSentimentAnalyzer <Path to twitter credential file>");
      System.exit(1);
    }
    if (conn == null) {
      System.err.println("Cannot establish a connection to database. Quitting...");
      System.exit(1);
    }
    String twitterCredentialFilePath = args[0];
    TwitterConfigUtil.setTwitterConfig(twitterCredentialFilePath);

    SparkConf conf = new SparkConf().setAppName("TwitterHashtagCollector").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    final JavaStreamingContext jssc = new JavaStreamingContext(sc, Seconds.apply(10));

    JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

    getScoreAndWriteToDB(twitterStream);

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

    try {
      conn.close();
    } catch (SQLException e) {
      System.err.println("Error while closing MySQL db connection: " + e);
    }
  }

  //TODO: need to use a file instead of a DB, due to memory limit on free-tier Amazon EC2 machine. (can't run Spark there)
  private static void getScoreAndWriteToDB(JavaReceiverInputDStream<Status> twitterStream) {
    //Filter out tweets that are not written in English, and tweets not posted in US
    JavaDStream<Status> englishTweetsInUS = twitterStream
        .filter(status -> status.getGeoLocation() != null)
        .filter(status -> isRoughlyWithinUSAMainland(status.getGeoLocation()))
        .filter(status -> status.getLang().equals(englishIsoLanguageCode));

    //map to lists of all words and the geo-location of author from each tweet.
    JavaPairDStream<List<String>, GeoLocation> wordsAndGeoLocationInTweets = englishTweetsInUS
        .mapToPair(status -> new Tuple2<>(getAsWordsList(status.getText()), status.getGeoLocation()));

    //calculate sentiment score from the lists of words, and map to a sentiment score and geo-location.
    JavaPairDStream<Double, GeoLocation> scoreAndGeoLocation = wordsAndGeoLocationInTweets
        .mapToPair(wordsAndGeoLocation ->
            new Tuple2<>(getSentimentScore(wordsAndGeoLocation._1), wordsAndGeoLocation._2));

    //write the result to database or file
    scoreAndGeoLocation.foreachRDD(rdd -> {
      rdd.foreach(TwitterSentimentAnalyzer::writeToDatabase);
    });
  }

  //TODO: need to use a file instead of a DB, due to memory limit on free-tier Amazon EC2 machine. (can't run Spark there)
  private static void writeToDatabase(Tuple2<Double, GeoLocation> tuple) throws SQLException {
    assert conn != null; //null check already done in main, but IntelliJ complains.
    PreparedStatement st = conn.prepareStatement(
        "INSERT INTO twitter.sentimentByState " +
            " (sentimentScore, latitude, longitude) " +
            "VALUES (?, ?, ?)");

    double score = tuple._1;
    double latitude = tuple._2.getLatitude();
    double longitude = tuple._2.getLongitude();

    st.setDouble(1, score);
    st.setDouble(2, latitude);
    st.setDouble(3, longitude);

    st.executeUpdate();
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

  //Establish DB connection
  //TODO: need to use a file instead of a DB, due to memory limit on free-tier Amazon EC2 machine. (can't run Spark there)
  public static Connection getDBConnection(String hostname, String user, String password) {
    try {
      MysqlDataSource dataSource = new MysqlDataSource();
      dataSource.setUser(user);
      dataSource.setPassword(password);
      dataSource.setServerName(hostname);

      return dataSource.getConnection();
    } catch (SQLException e) {
      // handle any errors
      System.err.println("SQLException: " + e);
    }
    return null;
  }
}
