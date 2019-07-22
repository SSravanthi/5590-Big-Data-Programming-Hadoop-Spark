import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object PageRank {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("PageRank")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("PageRank")
      .config(conf =conf)
      .getOrCreate()

    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //Importing the edges dataset and groups dataset
    val edges_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/Sravanthi Somalaraju/Documents/Bigdata ICPs/Module2/LAB2/nashville-meetup/group-edges.csv")

    val groups_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/Sravanthi Somalaraju/Documents/Bigdata ICPs/Module2/LAB2/nashville-meetup/meta-groups.csv")



    // Printing the Schema

    edges_data.printSchema()

    groups_data.printSchema()

    //Create or Replace temp view
    edges_data.createOrReplaceTempView("edge")

    groups_data.createOrReplaceTempView("group")


    val ngroup = spark.sql("select * from group")

    val nedge = spark.sql("select * from edge")
    //Renaming the columns and distinct
    val vertices = ngroup
      .withColumnRenamed("group_id", "id").limit(100)
      .distinct()

    val edges = nedge
      .withColumnRenamed("group1", "src").limit(500).distinct()
      .withColumnRenamed("group2", "dst").limit(500).distinct()

    //Output Dataframe
    val graph = GraphFrame(vertices, edges)

    edges.cache()
    vertices.cache()
    graph.vertices.show()
    graph.edges.show()


    println("Total Number of vertices: " + graph.vertices.count)
    println("Total Number of edges: " + graph.edges.count)

    //PageRank with tolerance
    val stationPageRank = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()

    //PageRank with max Iterations
    val maxIterationsPageRank = graph.pageRank.resetProbability(0.15).maxIter(10).run()
    maxIterationsPageRank.vertices.show()
    maxIterationsPageRank.edges.show()

    //PageRank for a personalized vertice
    val personalizedPageRank = graph.pageRank.resetProbability(0.15).maxIter(10).sourceId("6335372").run()
    personalizedPageRank.vertices.show()
    personalizedPageRank.edges.show()

    //Saving to File
    graph.vertices.write.csv("/Users/Sravanthi Somalaraju/Documents/Bigdata ICPs/Module2/LAB2/nashville-meetup/vertices")
    graph.edges.write.csv("/Users/Sravanthi Somalaraju/Documents/Bigdata ICPs/Module2/LAB2/nashville-meetup/edges")




  }

}