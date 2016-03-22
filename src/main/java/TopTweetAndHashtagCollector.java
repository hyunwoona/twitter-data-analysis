import scala.Tuple2;
import twitter4j.Status;
import util.TwitterStreamUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
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
 * Creates a stream using Twitter Streaming API, gets top hashtags and tweets, and writes to a file.
 */
public class TopTweetAndHashtagCollector {

  private static final DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
  private static final String timeZoneId = "America/Los_Angeles";
  private static int streamBatchDurationInSec = 60; //receive data at 60 second intervals
  private static int windowDurationInSec = 300; //Top hashtags and tweets in 5 minutes
  //output top 25 hashtags and top 3 tweets
  private static final int numTopHashtags = 25;
  private static final int numTopTweets = 3;

  //args[0]: full path to a textfile that contains a twitter login. See twitter4j.properties.template
  //args[1]: full path to a textfile to write hashtags and tweets to
  public static void main(String[] args) {
    //Logger.getLogger("org").setLevel(Level.OFF);
    //Logger.getLogger("akka").setLevel(Level.OFF);

    if (args.length != 2) {
      System.err.println("Usage: TopTweetAndHashtagCollector <Path to twitter credential file> <Output file path>");
      System.exit(1);
    }
    String twitterCredentialFilePath = args[0];
    String hashtagAndTweetOutputFilePath = args[1];

    //create Spark Streaming context, and get a stream of Twitter Status
    final JavaStreamingContext jssc = TwitterStreamUtils.getJavaStreamingContext("TopTweetAndHashtagCollector",
        twitterCredentialFilePath, streamBatchDurationInSec);
    JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

    File hashtagAndTweetOutputFile = new File(hashtagAndTweetOutputFilePath); //output file

    //get <hashtag count, hashtag> pairs, and write the top {numTopHashtags} to file
    JavaPairDStream<Long, String> topHashtagsDescendingOrdered = getTopHashtags(twitterStream);
    topHashtagsDescendingOrdered.foreachRDD(rdd -> {
      writeTopHashtagsToFile(hashtagAndTweetOutputFile, numTopHashtags, rdd);
    });

    //get <retweet count, tweet text> pairs, and write the top {numTopTweets} to file
    JavaPairDStream<Integer, String> topTweetsDescendingOrdered = getMostRetweetedTweets(twitterStream);
    topTweetsDescendingOrdered.foreachRDD(rdd -> {
      writeTopTweetsToFile(hashtagAndTweetOutputFile, numTopTweets, rdd);
    });

    //start computation and wait for termination
    TwitterStreamUtils.startAndWaitTermination(jssc);
  }

  //-Methods below are methods for top hashtags

  //Takes a Twitter stream, gets the {numTopHashtags} hashtags with the highest counts, and write to {file}
  public static JavaPairDStream<Long, String> getTopHashtags(JavaReceiverInputDStream<Status> twitterStream) {
    JavaDStream<String> statusTexts = twitterStream.map(Status::getText);
    JavaDStream<String> hashtags = extractHashtagFromTexts(statusTexts);

    JavaPairDStream<String, Long> hashtagAndCount = getHashtagAndCount(hashtags);
    return sortByDescendingHashtagCount(hashtagAndCount);
  }

  // extract all hashtags from status texts
  private static JavaDStream<String> extractHashtagFromTexts(JavaDStream<String> statusTextsWithHashtag) {
    return statusTextsWithHashtag
        .filter(statusText -> statusText.contains("#")) //filter out tweets without hashtag
        .flatMap(statusText -> {
          List<String> words = Arrays.asList(statusText.split("[ .,:\n\t]"));

          return words.stream()
              .filter(hashtagWord -> hashtagWord.startsWith("#")) //filter non-hashtag words
              .map(hashtagWord -> hashtagWord.replace("#", "")) //remove '#' character from hashtag words
              .collect(Collectors.toList());
        });
  }

  // count hashtags, get <hashtag, count> pair.
  private static JavaPairDStream<String, Long> getHashtagAndCount(JavaDStream<String> hashtags) {
    return hashtags
        .countByValue() //reduce RDDs of hashtags in DStream to PairRDDs of (hashtag, count)s
        .reduceByKeyAndWindow((count1, count2) -> count1 + count2,
            Seconds.apply(windowDurationInSec)); //further reduce by hashtag to get counts across RDDs
  }

  //take a <hashtag, count> pair, swap orders in tuple, sort them from ones with the largest counts to the smallest
  private static JavaPairDStream<Long, String> sortByDescendingHashtagCount(
      JavaPairDStream<String, Long> hashtagAndCount) {
    return hashtagAndCount
        .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)) //swap order in <hashtag, count> tuple
        .transformToPair(rdd -> rdd.sortByKey(false)); //sort in descending order of count
  }

  private static void writeTopHashtagsToFile(File file, int numTopHashtags,
                                             JavaPairRDD<Long, String> hashtagRDD) throws IOException {
    DateTime currentTime = DateTime.now(DateTimeZone.forID(timeZoneId));
    String timeRange = dateTimeFormat.print(currentTime.minusSeconds(windowDurationInSec))
        + " to " + dateTimeFormat.print(currentTime) + " (Pacific Time)";
    Files.append("Top " + numTopHashtags + " hashtags captured from " + timeRange + ":\n",
        file, Charset.forName("UTF-8"));

    // Get top {numTopHashtags} hashtags as (count, hashtag) tuple.
    List<Tuple2<Long, String>> topHashtags = hashtagRDD.take(numTopHashtags);
    topHashtags.forEach(countAndHashtag -> {
      try {
        // write count and hashtag to the end of file
        Files.append(countAndHashtag.toString() + "\n", file, Charset.forName("UTF-8"));
      } catch (IOException e) {
        System.err.println("error while writing output to file");
      }
    });
  }

  //Methods below are methods for most retweeted tweets

  //Takes a Twitter stream, gets the {numTopTweets} hashtags with the highest counts, and write to {file}
  public static JavaPairDStream<Integer, String> getMostRetweetedTweets(JavaReceiverInputDStream<Status> twitterStream) {
    JavaDStream<Status> retweetedTweets = twitterStream.filter(Status::isRetweet); //only retweeted tweets are considered

    JavaPairDStream<Integer, String> retweetCountAndText = getCountAndText(retweetedTweets);
    return sortByDescendingRetweetCount(retweetCountAndText);
  }

  //get <retweet count, tweet text> pairs from twitter status. Also remove newline from text
  private static JavaPairDStream<Integer, String> getCountAndText(JavaDStream<Status> retweetedTweets) {
    return retweetedTweets.mapToPair(status ->
        new Tuple2<>(status.getRetweetedStatus().getRetweetCount(), status.getText().replace("\n", " ")));
  }

  //sort the <retweet count, text> from ones with the most retweeted tweets to the least.
  private static JavaPairDStream<Integer, String> sortByDescendingRetweetCount(
      JavaPairDStream<Integer, String> retweetCountAndText) {
    return retweetCountAndText.window(Minutes.apply(windowDurationInSec)) //capture the stream in the specified window
        .transformToPair(rdd -> rdd.distinct()) //remove duplicates
        .transformToPair(rdd -> rdd.sortByKey(false)); //sort from the most retweeted tweets to the least retweeted ones
  }

  private static void writeTopTweetsToFile(File file, int numTopTweets,
                                           JavaPairRDD<Integer, String> tweetsRdd) throws IOException {
    Files.append(numTopTweets + " most retweeted Tweets in this time window:\n",
        file, Charset.forName("UTF-8"));

    // Get top {numTopTweets} tweets as (retweet count, tweet text) tuple.
    List<Tuple2<Integer, String>> topTweets = tweetsRdd.take(numTopTweets);
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
  }
}