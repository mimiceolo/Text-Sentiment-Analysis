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

import com.mongodb.client.{MongoClients, MongoClient, MongoDatabase, MongoCollection}
import org.bson.Document
import java.util.{Date, UUID}

import scala.collection.JavaConverters._

// spark-submit \
//   --class SVM_Streaming_MongoDB \
//   --master "local[4]" \
//   --driver-memory 4g \
//   --executor-memory 4g \
//   --packages \
// org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,\
// org.mongodb:mongodb-driver-sync:4.11.1 \
//   target/scala-2.12/sentimentanalysisstreaming_2.12-1.0.jar

object SVM_Streaming_MongoDB {

    // MongoDB connection (singleton)
    @transient lazy val mongoClient: MongoClient = MongoClients.create("mongodb://localhost:27017")
    @transient lazy val database: MongoDatabase = mongoClient.getDatabase("sentiment_analysis")
    @transient lazy val predictionsCollection: MongoCollection[Document] = database.getCollection("predictions")
    @transient lazy val metricsCollection: MongoCollection[Document] = database.getCollection("batch_metrics")
    
    // Helper method to ensure MongoDB connection is accessible
    def getPredictionsCollection: MongoCollection[Document] = {
        try {
            predictionsCollection
        } catch {
            case e: Exception =>
                println(s"[SVM-MongoDB] Error accessing predictions collection: ${e.getMessage}")
                throw e
        }
    }
    
    def getMetricsCollection: MongoCollection[Document] = {
        try {
            metricsCollection
        } catch {
            case e: Exception =>
                println(s"[SVM-MongoDB] Error accessing metrics collection: ${e.getMessage}")
                throw e
        }
    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
            .setAppName("Sentiment Analysis Streaming SVM - MongoDB")
            .set("spark.streaming.kafka.maxRatePerPartition", "1000")

        val ssc = new StreamingContext(conf, Seconds(10))
        ssc.checkpoint("hdfs://localhost:9000/user/spark/checkpoints_svm_mongo")

        val spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        import spark.implicits._

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "spark-svm-streaming-mongo",
            "auto.offset.reset" -> "earliest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array("tweets-testing")

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        // Load pre-trained model
        val model = try {
            PipelineModel.load("hdfs://localhost:9000/user/spark/models/svm_model")
        } catch {
            case e: Exception =>
                println("SVM Model not found. Please train model first.")
                System.exit(1)
                null
        }

        // Explicitly initialize MongoDB connection
        try {
            val testConnection = predictionsCollection
            val testMetrics = metricsCollection
            println("✓ MongoDB connection initialized and tested")
        } catch {
            case e: Exception =>
                println(s"✗ MongoDB connection failed: ${e.getMessage}")
                e.printStackTrace()
                System.exit(1)
        }
        
        println("✓ Model loaded successfully")
        println("Starting to process batches...")

        // Process each batch
        stream.foreachRDD { rdd =>
            if (!rdd.isEmpty()) {
                val batchId = UUID.randomUUID().toString
                val batchStartTime = System.currentTimeMillis()

                println(s"\n[SVM-MongoDB] Processing batch $batchId with ${rdd.count()} records")

                try {
                    // Parse JSON and clean text
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
                        .withColumn("original_tweet_id", col("ItemID"))
                        .select("original_tweet_id", "label", "tweet")

                    if (tweetsDF.count() > 0) {
                        // Make predictions
                        val predictions = model.transform(tweetsDF)

                        // Calculate batch metrics
                        val predictionAndLabels = predictions
                            .select("prediction", "label")
                            .rdd
                            .map(r => (r.getDouble(0), r.getDouble(1)))

                        val metrics = new MulticlassMetrics(predictionAndLabels)
                        val accuracy = metrics.accuracy
                        val f1Score = metrics.weightedFMeasure
                        val precision = metrics.weightedPrecision
                        val recall = metrics.weightedRecall

                        val batchEndTime = System.currentTimeMillis()
                        val processingTime = batchEndTime - batchStartTime

                        println(s"[SVM-MongoDB] Batch $batchId - Accuracy: $accuracy, F1: $f1Score")

                        // Convert predictions to MongoDB documents
                        println(s"[SVM-MongoDB] Converting predictions to MongoDB documents...")
                        val predictionsRows = predictions
                            .select("original_tweet_id", "tweet", "label", "prediction")
                            .collect()
                        
                        println(s"[SVM-MongoDB] Collected ${predictionsRows.length} prediction rows")
                        
                        val predictionsList = predictionsRows
                            .map { row =>
                                new Document()
                                    .append("tweet_id", row.getString(0))
                                    .append("text", row.getString(1))
                                    .append("true_label", row.getDouble(2).toInt)
                                    .append("predicted_label", row.getDouble(3).toInt)
                                    .append("model", "svm")
                                    .append("batch_id", batchId)
                                    .append("timestamp", new Date(batchStartTime))
                                    .append("correct", row.getDouble(2) == row.getDouble(3))
                            }
                            .toList.asJava
                        
                        println(s"[SVM-MongoDB] Created ${predictionsList.size()} MongoDB documents")

                        // Write predictions to MongoDB using Java driver
                        // Note: Duplicate tweet_ids are allowed since both SVM and NB models predict the same tweets
                        try {
                            if (!predictionsList.isEmpty) {
                                println(s"[SVM-MongoDB] Attempting to write ${predictionsList.size()} predictions to MongoDB...")
                                val collection = getPredictionsCollection
                                println(s"[SVM-MongoDB] Collection name: ${collection.getNamespace.getCollectionName}")
                                val result = collection.insertMany(predictionsList)
                                println(s"[SVM-MongoDB] ✓ Successfully wrote ${predictionsList.size()} predictions to MongoDB predictions collection")
                            } else {
                                println(s"[SVM-MongoDB] ⚠ No predictions to write (predictionsList is empty)")
                            }
                        } catch {
                            case e: Exception =>
                                println(s"[SVM-MongoDB] ✗ MongoDB predictions write failed: ${e.getMessage}")
                                println(s"[SVM-MongoDB] Exception type: ${e.getClass.getName}")
                                e.printStackTrace()
                        }

                        // Write batch metrics to MongoDB
                        val batchMetricsDoc = new Document()
                            .append("batch_id", batchId)
                            .append("model", "svm")
                            .append("accuracy", accuracy)
                            .append("f1_score", f1Score)
                            .append("precision", precision)
                            .append("recall", recall)
                            .append("total_tweets", tweetsDF.count().toInt)
                            .append("processing_time_ms", processingTime)
                            .append("timestamp", new Date(batchStartTime))

                        try {
                            println(s"[SVM-MongoDB] Attempting to write batch metrics to MongoDB...")
                            val collection = getMetricsCollection
                            println(s"[SVM-MongoDB] Metrics collection name: ${collection.getNamespace.getCollectionName}")
                            collection.insertOne(batchMetricsDoc)
                            println(s"[SVM-MongoDB] ✓ Successfully wrote batch metrics to MongoDB batch_metrics collection")
                        } catch {
                            case e: Exception =>
                                println(s"[SVM-MongoDB] ✗ Batch metrics write failed: ${e.getMessage}")
                                println(s"[SVM-MongoDB] Exception type: ${e.getClass.getName}")
                                e.printStackTrace()
                        }

                        // Also save to HDFS as backup
                        predictions
                            .select("tweet", "label", "prediction")
                            .write
                            .mode("append")
                            .parquet("hdfs://localhost:9000/user/spark/streaming_results/svm/")

                        println(s"[SVM-MongoDB] ✓ Batch complete in ${processingTime}ms")
                    }
                } catch {
                    case e: Exception =>
                        println(s"[SVM-MongoDB] ✗ Error processing batch: ${e.getMessage}")
                        e.printStackTrace()
                }
            }
        }

        println("\n✓ Starting Spark Streaming with MongoDB integration...")
        ssc.start()
        ssc.awaitTermination()
    }
}

