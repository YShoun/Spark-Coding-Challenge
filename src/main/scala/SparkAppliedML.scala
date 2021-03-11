import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{when, _}

/**
 * PART 3 : Spark Applied Machine Learning
 */
object SparkAppliedML {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutils/")
    val pathToFile = "data/"
    val pathToSavedFile = "out/"

    val conf = new SparkConf()
      .setAppName("TruataChallengePART3")
      .setMaster("local[*]")

    val ss = SparkSession.builder()
      .appName(name = "TCP3")
      .config(conf)
      .getOrCreate()

    // Read CSV iris and add column names
    val schema = new StructType()
      .add("sepal_length",FloatType,true)
      .add("sepal_width",FloatType,true)
      .add("petal_length",FloatType,true)
      .add("petal_width",FloatType,true)
      .add("class",StringType,true)

    val iris_raw = ss.read.schema(schema)
                          .csv(pathToFile+"iris.csv")
    // iris_raw.show()

    // convert the label into numeric values
    val iris_mod = iris_raw.withColumn(colName = "class_int",                                        // new column
                                        when(col("class") === "Iris-setosa",0)       // Iris Setosa = 0
                                        .when(col("class") === "Iris-versicolor",1)  // Iris Versicolour = 1
                                        .otherwise(2))                                        // Iris Virginica = 2
    // set features
    val assembler = new VectorAssembler()
      .setInputCols(Array("sepal_length","sepal_width","petal_length","petal_width"))
      .setOutputCol("features")

    // Build the logistic regression model
    val logisticRegression = new LogisticRegression()
      .setFeaturesCol("features")   // setting features column
      .setLabelCol("class_int")         // setting label column
      .setMaxIter(100)
      .setRegParam(100000)
      .setFitIntercept(true)

    //creating pipeline
    val pipeline = new Pipeline().setStages(Array(assembler,logisticRegression))

    //fitting the model
    val logisticRegressionModel = pipeline.fit(iris_mod)

    // Create testing data
    val testData = Seq(
      Row(5.1, 3.5, 1.4, 0.2),
      Row(6.2, 3.4, 5.4, 2.3))

    val testSchema = List(
      StructField("sepal_length", DoubleType, true),
      StructField("sepal_width", DoubleType, true),
      StructField("petal_length", DoubleType, true),
      StructField("petal_width", DoubleType, true))

    val pred_data = ss.createDataFrame(ss.sparkContext.parallelize(testData),StructType(testSchema))

    val predictions = logisticRegressionModel.transform(pred_data)
    predictions.show()

    //Export result to CSV
    predictions
      .select(col = "prediction")
      .repartition(1)
      .write.option("header", "true")
      .csv(pathToSavedFile+"out_3_2.txt")
  }
}
