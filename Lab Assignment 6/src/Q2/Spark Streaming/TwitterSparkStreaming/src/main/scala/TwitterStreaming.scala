import java.io.{PrintWriter, File}

import edu.umkc.fv.NLPUtils._
import edu.umkc.fv.Utils._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by pradyumnad on 07/07/15.
  * Modified by smoeller on March 1 2015
 */
object TwitterStreaming {

  def main(args: Array[String]) {

    val filterKeywords = Array("happy", "sad", "hungry")

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "DonYPpv8jYbKmR3LTp1XNAp3N")
    System.setProperty("twitter4j.oauth.consumerSecret", "vZ7dZezI9f0kz1sRQ5UMx5AQ51dinRNwyNB4vTdYyYibXIGpk0")
    System.setProperty("twitter4j.oauth.accessToken", "273143494-ZVxAo8cXoGyKvPj7gdKaEfFEPeiB42gkjvuBvVE0")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "KULdNgAHbcPKAmuKUTe7wjQelShXn8g1bBWq0TRTLNaOi")

    //Create a spark configuration with a custom name and master
    //For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("SMoellerTweetsApp").setMaster("local[*]")

    //Create a Streaming Context with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sc = ssc.sparkContext

    //Create a twitter stream using our list of applicable keywords
    val twitterStream = TwitterUtils.createStream(ssc, None, filterKeywords)
    println("Twitter stream started")


    //Loop from here to ENDTRAINING, with different keywords each time
    for (keyword <- filterKeywords) {
      println("Training: " + keyword)

      //convert the stream into a list, filtering down to just those tweets that contain
      //this particular key word
      val tweeters = twitterStream.flatMap(x => x.getText.split(" ")).filter(_.contains(keyword))

      //At this point tweeters is a list (array) of words from the tweets that contained
      //the current keyword
      tweeters.print()

      //This returns a list of words and their frequency count as a new list
      val topCounts10 = tweeters.reduceByWindow(_ + _, Seconds(10), Seconds(2)).map((_, 1))
      println("10 second reduction " + keyword + ": ")

      //This should print the first 10 words in the topCounts10 list, along with their frequency
      //Note that this prints the first 10 because that is the definition of print()
      topCounts10.print()

      //This saves the list of words and frequencies to a training directory named after
      //this keyword
      //ERROR: For some reason this is not saving anything to disk
      topCounts10.saveAsTextFiles("data/training/" + keyword + "/")
    }
    //ENDTRAINING

/*
    //Now to analyze the training data
    var model: NaiveBayesModel = null

    //This will return a list of directory names in the training directory, ordered
    //with a unique numerical index assigned to each name
    val labelToNumeric = createLabelMap("data/training/")

    //training is a collection of RDDs, with a separate RDD for each file found
    //in the training directory
    val training = sc.wholeTextFiles("data/training/*")
      .map(rawText => createLabeledDocument(rawText, labelToNumeric))

    val X_train = tfidfTransformer(training)

    X_train.foreach(vv => println(vv))
    model = NaiveBayes.train(X_train, lambda = 1.0)


    //Next collect data unsorted by keyword
    println("Collecting unfiltered tweets")
    val filter = Array("directions")
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val tweets = TwitterUtils.createStream(ssc, None, filter)
    //Get all of the tweet data that matches the filter
    val tweetData = tweets.flatMap(status => status.getText.split(" "))
    //Now to write all of the messages to training files
    tweetData.saveAsTextFiles("data/testing/")


    println("identifying unfiltered tweets")
    //Last, analyze the raw data
    val lines=sc.wholeTextFiles("data/testing/*")
    val data = lines.map(line => {
      val test = createLabeledDocumentTest(line._2, labelToNumeric)
      println(test.body)
      test
    })
    val X_test = tfidfTransformerTest(sc, data)
    val predictionAndLabel = model.predict(X_test)
    println("PREDICTION")
    predictionAndLabel.foreach(x => {
      labelToNumeric.foreach {
        y => if (y._2 == x) {
          println(y._1)
        }
      }
    })
*/
*/ */
    //Now kick it all off
    ssc.start()

    //var s:String="Twitter feed complete\n"
    //SocketClient.sendCommandToAndroid(s)
    println("Program complete")

    ssc.awaitTermination()
  }
}

