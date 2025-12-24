import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, StopWordsRemover}
import org.apache.spark.ml.classification.{NaiveBayes, LinearSVC}

// spark-submit \
//   --class TrainModels \
//   --master "local[4]" \
//   --driver-memory 4g \
//   --executor-memory 4g \
//   target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

object TrainModels {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
            .appName("Train Models Offline")
            .getOrCreate()

        import spark.implicits._

        println("=" * 60)
        println("Training Sentiment Analysis Models")
        println("=" * 60)

        // Check if preprocessed data exists, otherwise use CSV
        val dataPath = if (args.length > 0) args(0) else "hdfs://localhost:9000/user/preprocessed/"
        
        val trainingData = try {
            // Try to load preprocessed parquet data
            println(s"Trying to load preprocessed data from: $dataPath")
            spark.read
                .parquet(dataPath)
                .select($"Sentiment".cast("double").as("label"), $"SentimentText".as("tweet"))
        } catch {
            case e: Exception =>
                println(s"Preprocessed data not found: ${e.getMessage}")
                println("Loading from CSV instead...")
                
                // Load from CSV and preprocess
                val csvPath = "hdfs://localhost:9000/user/kafka_data/tweets-training/"
                println(s"Reading CSV from: $csvPath")
                
                val rawDF = spark.read
                    .option("header", "false")
                    .option("inferSchema", "true")
                    .csv(csvPath)
                
                // Detect number of columns and rename accordingly
                val numColumns = rawDF.columns.length
                println(s"Detected $numColumns columns in CSV")
                
                val renamedDF = numColumns match {
                    case 4 => rawDF.toDF("ItemID", "Sentiment", "SentimentSource", "Text")
                    case 5 => rawDF.toDF("ItemID", "Sentiment", "SentimentSource", "Text", "timestamp")
                    case 6 => rawDF.toDF("sentiment", "id", "date", "query", "user", "text")
                              .select($"id".as("ItemID"), $"sentiment".as("Sentiment"), 
                                     lit("Sentiment140").as("SentimentSource"), $"text".as("Text"))
                    case _ => throw new IllegalArgumentException(s"Unexpected number of columns: $numColumns")
                }
                
                renamedDF.select(
                        when($"Sentiment" === "4", 1.0)
                            .when($"Sentiment" === "1", 1.0)
                            .otherwise(0.0).as("label"),
                        // Clean text
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(trim($"Text")),
                                        "(?i)(https?:\\/\\/[^\\s]+|www\\.[^\\s]+)", ""
                                    ),
                                    "(#|@|&)[^\\s]+", ""
                                ),
                                "\\d+", ""
                            ),
                            "[^a-zA-Z ]", " "
                        ).as("tweet")
                    )
                    .filter($"tweet".isNotNull && length($"tweet") > 0)
        }

        println(s"Training data loaded: ${trainingData.count()} records")
        trainingData.show(5, truncate = false)

        // Check class distribution
        println("\nClass distribution:")
        trainingData.groupBy("label").count().show()

        // Sample data if too large (for faster training)
        val sampledData = if (trainingData.count() > 100000) {
            println("Sampling 100,000 records for training...")
            trainingData.sample(false, 100000.0 / trainingData.count()).limit(100000)
        } else {
            trainingData
        }

        // Cache the data for faster training
        sampledData.cache()

        // Create pipeline stages
        val tokenizer = new Tokenizer()
            .setInputCol("tweet")
            .setOutputCol("words")

        val stopWordsRemover = new StopWordsRemover()
            .setInputCol("words")
            .setOutputCol("filtered")

        val hashingTF = new HashingTF()
            .setInputCol("filtered")
            .setOutputCol("rawFeatures")
            .setNumFeatures(10000)

        val idf = new IDF()
            .setInputCol("rawFeatures")
            .setOutputCol("features")
            .setMinDocFreq(2)

        println("\n" + "=" * 60)
        println("Training Naive Bayes Model")
        println("=" * 60)
        
        // Train Naive Bayes
        val nb = new NaiveBayes()
            .setSmoothing(1.0)
            .setModelType("multinomial")
        
        val nbPipeline = new Pipeline()
            .setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, nb))

        val nbModel = nbPipeline.fit(sampledData)
        
        val nbModelPath = "hdfs://localhost:9000/user/spark/models/nb_model"
        println(s"Saving Naive Bayes model to: $nbModelPath")
        nbModel.write.overwrite().save(nbModelPath)
        println("✓ Naive Bayes model saved successfully")

        println("\n" + "=" * 60)
        println("Training SVM Model")
        println("=" * 60)
        
        // Train SVM
        val svm = new LinearSVC()
            .setMaxIter(10)
            .setRegParam(0.1)

        val svmPipeline = new Pipeline()
            .setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, svm))

        val svmModel = svmPipeline.fit(sampledData)
        
        val svmModelPath = "hdfs://localhost:9000/user/spark/models/svm_model"
        println(s"Saving SVM model to: $svmModelPath")
        svmModel.write.overwrite().save(svmModelPath)
        println("✓ SVM model saved successfully")

        println("\n" + "=" * 60)
        println("Model Training Complete!")
        println("=" * 60)
        println(s"Models saved to HDFS:")
        println(s"  - Naive Bayes: $nbModelPath")
        println(s"  - SVM:         $svmModelPath")
        println("=" * 60)

        spark.stop()
    }
}
