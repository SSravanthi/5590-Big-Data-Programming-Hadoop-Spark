import org.apache.log4j.{Level, Logger}
import org.apache.spark._


object MergeSort {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\winutils")


    //Controlling log level

    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)

    //Spark Context

    val conf =   new SparkConf().setAppName("mergeSort").setMaster("local");
    val sc   =   new SparkContext(conf);

    //array and parllalize to know no of partitions
    val a = Array(34, 55, 16, 9, 56, 11, 78, 19, 82);

    val b = sc.parallelize(Array(34, 55, 16, 9, 56, 11, 78, 19, 82));


    val maparray = b.map(x=>(x,1))

    val sorted = maparray.sortByKey();


    //Printing the RDD before Sort
    sorted.keys.collect().foreach(println)

    val n = a.length;

    val l1 = 0;

    val r = n-1;

    print("\nArray Before Sort\n")
    for ( x <- a)
    {
      print(x);
    }
    //for sorting
    sort(a,l1,r);

    print("\nArray after Sort\n")
    for ( y <- a)
    {
      print(y)
    }



    // Sorting Array
    def sort(arr: Array[Int], l1: Int, r: Int): Unit = {
      if (l1 < r) { // Find the middle point
        val m = (l1 + r) / 2
        // Sort first and second halves
        sort(arr, l1, m)
        sort(arr, m + 1, r)
        // Merge the sorted halves
        merge(arr, l1, m, r)
      }
    }


    // Merge for Array
    def merge(arr: Array[Int], l1: Int, m: Int, r: Int): Unit = {

      // Find sizes of two subarrays to be merged
      val n1 = m - l1 + 1
      val n2 = r - m


      /* Create temp arrays */
      val L = new Array[Int](n1)
      val R = new Array[Int](n2)

      /*Copy data to temp arrays*/

      var a = 0
      while (a < n1){
        L(a) = arr(l1 + a);
        a += 1;
      }
      var b = 0
      while (b < n2){
        R(b) = arr(m + 1 + b);
        b += 1;
      }


      /* Merge the temp arrays */
      // Initial indexes of first and second subarrays

      var i = 0
      var j = 0
      // Initial index of merged subarry array
      var k = l1
      while (i < n1 && j < n2) {
        if (L(i) <= R(j)) {
          arr(k) = L(i)
          i += 1
        }
        else {
          arr(k) = R(j)
          j += 1
        }
        k += 1
      }

      /* Copy remaining elements of L[] if any */
      while (i < n1)
      {
        arr(k) = L(i)
        i += 1
        k += 1
      }

      /* Copy remaining elements of R[] if any */
      while (j < n2)
      {
        arr(k) = R(j)
        j += 1
        k += 1
      }

    }

  }



}