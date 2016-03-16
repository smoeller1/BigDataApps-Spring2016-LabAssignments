import java.io.{PrintWriter, File}

import breeze.linalg.max
import edu.umkc.fv.NLPUtils._
import edu.umkc.fv.Utils._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Modified by smoeller on March 16 2015
 */
object TwitterStreaming {

  /* Set trainBool to 1 to collect fresh tweets of what brands and items users are talking about
     Set trainBool to 0 to recommend the top brand + item from the already collected tweets
   */
  val trainBool = 1


  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    //list of clothing items we are interested int
    val items = Array("shorts", "jeans", "slacks", "dress", "tshirt", "polo", "socks", "belt")

    //list of stores we may want to shop at
    val brands = Array("gap", "old navy", "banana republic", "aeropostale", "brooks brothers", "Express", "Eddie Bauer", "Buckle")

    //Join both lists for our twitter API filter
    val filterKeywords = items ++ brands

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

    if (trainBool == 1) {
      /* Collect new training data */

      //Create a twitter stream using our list of applicable keywords
      val twitterStream = TwitterUtils.createStream(ssc, None, filterKeywords)
      println("Twitter stream started")

      //Loop from here to ENDTRAINING, with different keywords each time,
      // building RDDs and saving them to disk
      for (keyword <- items) {
        for (brand <- brands) {
          println("looking for " + keyword + " in the " + brand + " store")

          //convert the stream into a list, filtering down to just those tweets that contain
          //this particular key word
          val tweeters = twitterStream.flatMap(x => x.getText.split(" ")).filter(_.contains(keyword)).filter(_.contains(brand))

          //At this point tweeters is a list (array) of words from the tweets that contained
          //the current keyword
          tweeters.print()

          //This returns a list of words and their frequency count as a new list
          //using an RDD size of 4 seconds (though this seems way too small for how
          //little data twitter actually returns...)
          val topCounts = tweeters.reduceByWindow(_ + _, Seconds(20), Seconds(4)).map((_, 1))
          println("20 second reduction " + keyword + ": ")

          //This should print the first 10 words in the topCounts10 list, along with their frequency
          //Note that this prints the first 10 because that is the definition of print()
          topCounts.print()

          //This saves the list of words and frequencies to a training directory named after
          //this keyword
          topCounts.saveAsTextFiles("data/training/" + keyword + "-" + brand + "/")
        }
      }
      //ENDTRAINING
    } else {
      /* analyze the training data & use it to recommend something */

      var model: NaiveBayesModel = null

      //This will return a list of directory names in the training directory, ordered
      //with a unique numerical index assigned to each name
      val labelToNumeric = createLabelMap("data/training/")

      //training is a collection of RDDs, with a separate RDD for each file found
      //in the training directory
      val training = sc.wholeTextFiles("data/training/*")
        .map(rawText => createLabeledDocument(rawText, labelToNumeric))

      //Apply a TFID transformation to the text
      val X_train = tfidfTransformer(training)

      //Now to runs the Naive Bayes model to build floating point recommendations
      model = NaiveBayes.train(X_train, lambda = 1.0)

      //Pick the most recommended item + store to recommend
      var recommended = max(model)
      println("The most tweeted item that we recommend you buy is " + recommended.toString)

    }

    //Now kick it all off
    ssc.start()

    //var s:String="Twitter feed complete\n"
    //SocketClient.sendCommandToAndroid(s)
    println("Program complete")

    ssc.awaitTermination()
  }
}

