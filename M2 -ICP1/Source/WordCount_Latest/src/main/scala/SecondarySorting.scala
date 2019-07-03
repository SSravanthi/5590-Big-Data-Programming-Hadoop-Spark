import org.apache.spark.{ SparkConf, SparkContext }
object SecondarySorting {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("secondarysorting").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val tempRDD = sc.textFile("C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\Module2\\WordCount_Latest\\input\\input1")
    // Pairs the list of tuples.
    val pairsRDD = tempRDD.map(_.split(",")).map { k => ((k(0), k(1)),k(3))}
    println("pairsRDD")
    pairsRDD.foreach { println }
    val numReducers = 3;
    // partioning the created tuples and sorted.
    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(k => k))
    println("listRDD")
    listRDD.foreach {
      println
    }
    listRDD.saveAsTextFile("Output1");
  }
}