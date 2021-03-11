import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

/**
 * PART 2 Spark DataFrame
 */
object SparkDataFrame {
  System.setProperty("hadoop.home.dir", "C:/winutils/")
  val pathToFile = "data/"
  val pathToSavedFile = "out/"

  val conf = new SparkConf()
    .setAppName("TruataChallengePART2")
    .setMaster("local[*]")

  val ss = SparkSession.builder()
    .appName(name = "TCP2")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    // TASK 1
    val airbnbRaw = loadData()
    airbnbRaw.show()

    // TASK 2
    aggMinMaxAvg(airbnbRaw)

    // TASK 3
    avgBedBathroom(airbnbRaw)

    //TASK 4
    accommodatedLowPriceHighRating(airbnbRaw)
  }

  /**
   * Task 1
   * Load data in a Spark DataFrame
   */
  def loadData(): DataFrame = {
    ss.read.parquet(pathToFile+"sf-airbnb-clean.parquet")
  }

  /**
   * Task 2
   * Create CSV output file under out/out_2_2.txt that lists the minimum price, maximum price, and total row count from
   * this dataset.
   *
   * @param airbnbRaw
   */
  def aggMinMaxAvg(airbnbRaw : DataFrame) = {
    // Register the DataFrame as a SQL temporary view
    airbnbRaw.createOrReplaceTempView("Table0")
    val airbnbRes0 = ss.sql(sqlText = "SELECT MIN(price) AS min_price, MAX(price) as max_price, COUNT(1) AS row_count FROM Table0")
    airbnbRes0.show()

    // export result to csv file
    airbnbRes0.repartition(1).write.option("header", "true").csv(pathToSavedFile+"out_2_2.txt")
  }

  /**
   * TASK 3
   * Calculate the average number of bathrooms and bedrooms across all the properties listed in this data set with
   * a price of > 5000 and a review score being exactly equal to 10.
   *
   * @param airbnbRaw
   */
  def avgBedBathroom(airbnbRaw : DataFrame) = {
    //getting the average score of all reviews and add it to DF
    val marksColumns = Array(
      col("review_scores_accuracy"),
      col("review_scores_cleanliness"),
      col("review_scores_checkin"),
      col("review_scores_communication"),
      col("review_scores_location"),
      col("review_scores_value"))
    val averageFunc = marksColumns.foldLeft(lit(0)){(x, y) => x+y}/marksColumns.length
    val airbnbMod = airbnbRaw.withColumn("review_avg_score", averageFunc)

    airbnbMod.createOrReplaceTempView("Table1")
    val airbnbRes1 = ss.sql(sqlText =
      "SELECT AVG(bathrooms) AS avg_bathrooms, AVG(bedrooms) as avg_bedrooms " +
        "FROM Table1 " +
        "WHERE price > 5000 AND review_avg_score = 10")
    airbnbRes1.show()

    // export result to csv file
    airbnbRes1.repartition(1).write.option("header", "true").csv(pathToSavedFile+"out_2_3.txt")
  }

  /**
   * TASK 4
   * How many people can be accommodated by the property with the lowest price and highest rating?
   * @param airbnbRaw
   */
  def accommodatedLowPriceHighRating(airbnbRaw : DataFrame) = {
    // We consider the highest rating based on the column review_scores_rating
    airbnbRaw.createOrReplaceTempView("Table2")
    val airbnbRes2 = ss.sql(sqlText =
      "SELECT accommodates "+
        "FROM Table2 "+
        "WHERE price = (SELECT MIN(price) FROM Table2) AND review_scores_rating = (SELECT MAX(review_scores_rating) FROM Table2)")
    airbnbRes2.show()

    // export result to csv file
    airbnbRes2.repartition(1).write.option("header", "true").csv(pathToSavedFile+"out_2_4.txt")
  }
}
