import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object MSort {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\winutils")

    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //Spark Context
    val conf = new SparkConf().setAppName("MSort").setMaster("local");
    val sc = new SparkContext(conf);
    //read input file
    //val input= sc.textFile("input1")
   //val ms=input.flatMap(line=>{line.split(",")})
    val input = Array(19, 14, 16, 22, 17, 1, 8);
    val ms = sc.parallelize(Array(39, 14, 56, 22, 17, 1, 8));
    val maparray = ms.map(x => (x, 1))

    //val maparray = ms.map(x => (x, 1))

    //maparray.collect().foreach(println)
    val sorted = maparray.sortByKey()
    //Printing the RDD before Sort
    sorted.keys.collect().foreach(println)
  }
}