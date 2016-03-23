#Top Tweets, Hashtags, and Sentiments using Twitter Streaming API and Spark

##Main Projects
Two main programs, [TopTweetAndHashtagCollector](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/java/TopTweetAndHashtagCollector.java) and 
[TwitterSentimentAnalyzer](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/java/TwitterSentimentAnalyzer.java).
Using TwitterSentimentAnalyzer, I also made a prototype of an interesting web app, [Tweet Stats](http://django-ericna.rhcloud.com/).

###TopTweetAndHashtagCollector
From a stream of tweets, gets top hashtags and tweets

###TwitterSentimentAnalyzer
From a stream of tweets, calculates the sentiment score of each tweet, and gets the sentiment score and geo-location of the author.

This sentiment score and geo-location is further processed on the client side, to look up the address by the latitude-longitude and get the ISO state code.

This is done by [get_sentiment_by_state.py](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/get_sentiment_by_state.py), written in Python3.

###Tweet Stats
[Tweet Stats](http://django-ericna.rhcloud.com/) is web-app hosted on a Python Django server.

It currently has a Geo-chart of average sentiment score of people in different states. I explained about the project in detail on the website.

##Running the Programs

###TopTweetAndHashtagCollector.java and TwitterSentimentAnalyzer.java

Takes two command-line arguments.
`Path to Twitter credential file`: full path to a textfile that contains a twitter login. See `twitter4j.properties.template`.

If you need these keys, please refer to [How to get API Keys and Tokens for Twitter](https://www.slickremix.com/docs/how-to-get-api-keys-and-tokens-for-twitter/).

`Output file path`: full path to a textfile to write sentiment analysis output to

Dependencies are contained in pom.xml

Some sample output files from `TopTweetAndHashtagCollector`: [Top Tweet and Hashtags 1](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/top_hashtags_And_tweets_2016032_234002.txt) [Top Tweet and Hashtags 2](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/top_hashtags_And_tweets_2016032_185101.txt) [Top Tweet and Hashtags 3](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/top_hashtags_And_tweets_2016032_122900.txt).

Some sample output files from `TwitterSentimentAnalyzer`: [200_lines](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/sample_sentiment_output_lines.csv) [3000_lines](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/sample_sentiment_3000_lines.csv).

###get_sentiment_by_state.py

Takes one command-line argument.

`Path to input file`: lines of comma-separated scores, latitudes, and longitudes. The input to this file is produced from `TwitterSentimentAnalyzer`.

You can use [200_lines](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/sample_sentiment_200_lines.csv) or [3000_lines](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/sample_sentiment_3000_lines.csv).

Output with these sample inputs: [output_200_lines](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/sample_sentiment_output_200_lines.json) or [output_3000_lines](https://github.com/hyunwoona/yewno-data-assignment/blob/master/src/main/resources/sentiment/sample_sentiment_output_3000_lines.json).
