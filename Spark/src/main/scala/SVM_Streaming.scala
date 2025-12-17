import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics


object SVM_Streaming {

    // Define schema for JSON parsing
    val tweetSchema = StructType(Array(
        StructField("ItemID", StringType, true),
        StructField("Sentiment", StringType, true),
        StructField("SentimentSource", StringType, true),
        StructField("Text", StringType, true),
        StructField("timestamp", LongType, true)
    ))

    def cleanText(text: String): String = {
        text.replaceAll("(?i)(https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|www\\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9]+\\.[^\\s]{2,}|www\\.[a-zA-Z0-9]+\\.[^\\s]{2,})", "")
            .replaceAll("(#|@|&).*?\\w+", "")
            .replaceAll("\\d+", "")
            .replaceAll("[^a-zA-Z ]", " ")
            .toLowerCase()
            .trim()
            .replaceAll("\\s+", " ")
    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
            .setAppName("Sentiment Analysis Streaming - SVM")
            .set("spark.streaming.kafka.maxRatePerPartition", "1000")

        val ssc = new StreamingContext(conf, Seconds(10))
        ssc.checkpoint("hdfs://localhost:9000/user/spark/checkpoints_svm")

        val spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        import spark.implicits._

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "spark-svm-streaming",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("tweets-testing")

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        // Load pre-trained SVM model
        val model = try {
            PipelineModel.load("hdfs://localhost:9000/user/spark/models/svm_model")
        } catch {
            case e: Exception =>
                println("SVM Model not found. Please train model first.")
                System.exit(1)
                null
        }

        stream.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                println(s"[SVM] Processing batch with ${rdd.count()} records")

                // Parse JSON using Spark SQL
                val jsonDF = spark.read.json(rdd.map(_.value()))
                
                val tweetsDF = jsonDF
                    .withColumn("label", col("Sentiment").cast("double"))
                    .withColumn("tweet", 
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(trim(col("Text"))),
                                        "(?i)(https?://[^\\s]+|www\\.[^\\s]+)", ""
                                    ),
                                    "(#|@|&)[^\\s]+", ""
                                ),
                                "\\d+", ""
                            ),
                            "[^a-zA-Z ]", " "
                        )
                    )
                    .select("label", "tweet")

                if (tweetsDF.count() > 0) {
                    val predictions = model.transform(tweetsDF)

                    val predictionAndLabels = predictions
                        .select("prediction", "label")
                        .rdd
                        .map(r => (r.getDouble(0), r.getDouble(1)))

                    val metrics = new MulticlassMetrics(predictionAndLabels)

                    println(s"[SVM] Batch Accuracy: ${metrics.accuracy}")
                    println(s"[SVM] Batch F1-Score: ${metrics.weightedFMeasure}")

                    predictions
                        .select("tweet", "label", "prediction")
                        .write
                        .mode("append")
                        .parquet("hdfs://localhost:9000/user/spark/streaming_results/svm/")
                }
            }
        }

        ssc.start()
        ssc.awaitTermination()
    }
}
