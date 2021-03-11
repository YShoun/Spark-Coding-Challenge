import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Part 1: Spark RDD API
 * All results are saved in out/out_1_2xxxxx.txt/part-00000 as RDD uses distributed computation
 */
object SparkRDD {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  val pathToFile = "data/"
  val pathToSavedFile = "out/"

  val conf =new SparkConf()
    .setAppName("TruataChallengePART1")
    .setMaster("local[*]") // use as many cores as available

  val sc = SparkContext.getOrCreate(conf)

  /**
   * Execute all 3 tasks of the coding challenge part 1
   * @param args
   */
  def main(args: Array[String]): Unit = {
    //TASK 1
    val groceriesRdd = loadDataRDD()
    groceriesRdd.take(num = 5).foreach(arr => println(arr.mkString(", "))) // As an RDD[Array[String]] we convert each Array -> String to print the content of it

    //TASK 2
    uniqueProductsToTxt(groceriesRdd)

    //TASK 3
    productFrequency(groceriesRdd)
  }

  /**
   * TASK 1
   * Read the CSV file
   * @return an RDD of the CSV
   */
  def loadDataRDD(): RDD[Array[String]] = {
    // read and return CSV as RDD
    sc.textFile(pathToFile+"groceries.csv")
      .map(_.split(","))
  }

  /**
   * TASK 2
   * Create a list of all (unique) products present in the transactions
   * Write out this list in out/out_1_2a.txt
   * Write out the total number of unique products in out/out_1_2b.txt
   * @param groceries
   */
  def uniqueProductsToTxt(groceries: RDD[Array[String]]) ={

    // Flatten the data and take distinct products
    val uniqueProduct = groceries.flatMap(x => x)
                                 .distinct()
    // export the result
    uniqueProduct.coalesce(numPartitions = 1, shuffle = true).saveAsTextFile(pathToSavedFile+"out_1_2a.txt")

    // Count total number of unique products
    val uniqueProductCount = uniqueProduct.count()

    //export the result
    sc.parallelize(Seq(uniqueProductCount)).coalesce(numPartitions = 1, shuffle = true).saveAsTextFile(pathToSavedFile+"out_1_2b.txt")

  }

  /**
   * TASK 3
   * Top 5 purchased products along with how often they were purchased (frequency count) in descending order of frequency
   * @param groceries
   */
  def productFrequency(groceries: RDD[Array[String]]) ={
    // Flatten the data
    val prodFreq = groceries.flatMap(x => x)
                            .map{w => (w,1)}
                            .reduceByKey(_+_)
                            .takeOrdered(num = 5){Ordering[Int].reverse.on(_._2)}

    sc.parallelize(prodFreq).coalesce(numPartitions = 1, shuffle = true).saveAsTextFile(pathToSavedFile+"out_1_2c.txt")
  }
}

