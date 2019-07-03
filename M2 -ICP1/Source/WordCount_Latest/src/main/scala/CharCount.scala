/**
  * Illustrates flatMap + countByValue for wordcount.
  */
import org.apache.spark._
object CharCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("charCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(""))
    // Transform into word and count.
    val counts = words.map(character => (character, 1)).reduceByKey{case (x, y) => x + y}.sortByKey(true,1)

    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output3")
    println("ok")



  }
}
