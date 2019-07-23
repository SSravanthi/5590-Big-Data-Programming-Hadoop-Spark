import org.apache.log4j.{Level, Logger}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
  * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
  * stream. The stream is instantiated with credentials and optionally filters supplied by the
  * command line arguments.

  */
object Task3 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming")

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    // Set s the logging level if log4j not configured (override by adding log4j.properties to classpath)
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("oauth.consumerKey", consumerKey)
    System.setProperty("oauth.consumerSecret", consumerSecret)
    System.setProperty("oauth.accessToken", accessToken)
    System.setProperty("oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("Spark Streaming")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    // Set the Spark StreamingContext to create a DStream for every 2 seconds
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    val stream_data = TwitterUtils.createStream(ssc, None, filters)


    stream_data.print()

    val hashTag = stream_data.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val Cnt60 = hashTag.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))

    Cnt60.print()



    ssc.start()
    ssc.awaitTermination()
  }
}