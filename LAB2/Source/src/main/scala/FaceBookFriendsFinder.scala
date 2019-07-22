import org.apache.spark._
import org.apache.log4j.{Level, Logger}

object FaceBookFriendsFinder{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("facebookfriendsfinder").setMaster("local[*]");
    val sc = new SparkContext(conf)

    /** Map function take a line like "5 0 4" and then generates pair of friends
        sorted by id (called the 'key') and output along with the entire friends list.
       Example: pairMapper("4 0 1 2 5") would return the following list
          (0, 4) => 0 1 2 5
          (1, 4) => 0 1 2 5
          (2, 4) => 0 1 2 5
           ( 4, 5) => 0 1 2 5
      */

    def fbfriendsMapper(line: String) = {
      val words = line.split(" ")
      val key = words(0)
      //val friend = words(1)
      val pairs = words.slice(1, words.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }


    /** Reduce function groups by the key and intersects the set with the accumulator to find
       common friends.*/

    def fbfriendsReducer(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set
    }
    /*Contents of /tmp/data.txt
     0 1 3 4 5
     1 0 2 4
     2 1 3 4
     3 0 2
     4 0 1 2 5
     5 0 4
     Output:
    (0,1) 4
    (0,4) 1 5
    (0,5) 4
    (1,2) 4
    (1,4) 0 2
    (2,4) 1
    (4,5) 0
    */

    //Read the input file
    val file = sc.textFile("FaceBookFriendFinder_Data/fbfriendsfinderinput")

    val results = file.flatMap(fbfriendsMapper)
      .reduceByKey(fbfriendsReducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} ${line._2.mkString(" ")}")})

    //Save Output as Textfile
    results.coalesce(1).saveAsTextFile("FaceBookCommonFriendsList")

  }

}

