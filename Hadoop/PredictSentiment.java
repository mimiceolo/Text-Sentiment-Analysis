import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import org.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * Testing-Only MapReduce Job (Real-time Prediction)
 * 
 * Loads pre-trained model and classifies streaming data
 * Usage: hadoop jar kafka_hadoop_nb.jar PredictSentiment <model_dir> <test_data_dir> <output_dir>
 */
public class PredictSentiment {
    
    public static enum Global_Counters {
        TWEETS_PROCESSED,
        TRUE_POSITIVE,
        FALSE_POSITIVE,
        TRUE_NEGATIVE,
        FALSE_NEGATIVE
    }

    /* 
     * Prediction Mapper
     * Loads model in setup(), then classifies each tweet
     */
    public static class Map_Predict extends Mapper<Object, Text, Text, Text> {
        
        private int features_size, tweets_size, pos_tweets_size, neg_tweets_size, pos_words_size, neg_words_size;
        private Double pos_class_probability, neg_class_probability;
        private HashMap<String, Double> pos_words_probabilities = new HashMap<>();
        private HashMap<String, Double> neg_words_probabilities = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String model_path = conf.get("model.path");
            
            if(model_path == null || model_path.isEmpty()) {
                throw new IOException("Model path not specified! Use -D model.path=<path>");
            }

            System.out.println("Loading model from: " + model_path);
            
            Path model_dir = new Path(model_path);
            FileSystem fs = model_dir.getFileSystem(conf);
            
            // Load metadata
            Path metadata_path = new Path(model_dir, "_metadata.txt");
            if(!fs.exists(metadata_path)) {
                throw new IOException("Model metadata not found: " + metadata_path);
            }
            
            BufferedReader metaReader = new BufferedReader(new InputStreamReader(fs.open(metadata_path)));
            String line;
            while((line = metaReader.readLine()) != null) {
                String[] parts = line.split("=");
                if(parts.length == 2) {
                    switch(parts[0]) {
                        case "tweets_size": tweets_size = Integer.parseInt(parts[1]); break;
                        case "pos_tweets_size": pos_tweets_size = Integer.parseInt(parts[1]); break;
                        case "neg_tweets_size": neg_tweets_size = Integer.parseInt(parts[1]); break;
                        case "pos_words_size": pos_words_size = Integer.parseInt(parts[1]); break;
                        case "neg_words_size": neg_words_size = Integer.parseInt(parts[1]); break;
                        case "features_size": features_size = Integer.parseInt(parts[1]); break;
                    }
                }
            }
            metaReader.close();
            
            pos_class_probability = ((double) pos_tweets_size) / tweets_size;
            neg_class_probability = ((double) neg_tweets_size) / tweets_size;
            
            System.out.println("Model metadata loaded:");
            System.out.println("  Vocabulary size: " + features_size);
            System.out.println("  Training tweets: " + tweets_size);
            
            // Load model weights
            HashMap<String, Integer> pos_words = new HashMap<>();
            HashMap<String, Integer> neg_words = new HashMap<>();
            
            FileStatus[] files = fs.listStatus(model_dir);
            for(FileStatus file : files) {
                if(file.isFile() && file.getPath().getName().startsWith("part-")) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                    String modelLine;
                    while((modelLine = br.readLine()) != null) {
                        String[] columns = modelLine.split("\t");
                        if(columns.length == 2) {
                            String[] counts = columns[1].split("@");
                            if(counts.length == 2) {
                                pos_words.put(columns[0], Integer.parseInt(counts[0]));
                                neg_words.put(columns[0], Integer.parseInt(counts[1]));
                            }
                        }
                    }
                    br.close();
                }
            }
            
            // Calculate probabilities with Laplace smoothing
            for(Map.Entry<String,Integer> entry : pos_words.entrySet()) {
                String word = entry.getKey();
                pos_words_probabilities.put(word, ((double) entry.getValue() + 1) / (pos_words_size + features_size));
                neg_words_probabilities.put(word, ((double) neg_words.get(word) + 1) / (neg_words_size + features_size));
            }
            
            System.out.println("✓ Model loaded: " + pos_words_probabilities.size() + " words");
        }
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            if(line.isEmpty()) 
                return;

            try {
                JSONObject json = new JSONObject(line);
                
                String tweet_id = json.getString("ItemID");
                String tweet_sentiment = json.getString("Sentiment");
                String tweet_text = json.getString("Text");
                long timestamp = json.optLong("timestamp", System.currentTimeMillis());

                // Clean text
                tweet_text = tweet_text.replaceAll("(?i)(https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|www\\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9]+\\.[^\\s]{2,}|www\\.[a-zA-Z0-9]+\\.[^\\s]{2,})", "")
                                    .replaceAll("(#|@|&).*?\\w+", "")
                                    .replaceAll("\\d+", "")
                                    .replaceAll("[^a-zA-Z ]", " ")
                                    .toLowerCase()
                                    .trim()
                                    .replaceAll("\\s+", " ");

                // Calculate log probabilities
                Double log_pos_probability = Math.log(pos_class_probability);
                Double log_neg_probability = Math.log(neg_class_probability);

                if(tweet_text != null && !tweet_text.trim().isEmpty()) {
                    String[] tweet_words = tweet_text.split(" ");
                    for(String word : tweet_words) {
                        if(pos_words_probabilities.containsKey(word)) {
                            log_pos_probability += Math.log(pos_words_probabilities.get(word));
                            log_neg_probability += Math.log(neg_words_probabilities.get(word));
                        }
                    }
                }

                // Classify
                String predicted_label;
                int predicted_value;
                if(Double.compare(log_pos_probability, log_neg_probability) > 0) {
                    predicted_label = "POSITIVE";
                    predicted_value = 1;
                    if(tweet_sentiment.equals("1"))
                        context.getCounter(Global_Counters.TRUE_POSITIVE).increment(1);
                    else
                        context.getCounter(Global_Counters.FALSE_POSITIVE).increment(1);
                } else {
                    predicted_label = "NEGATIVE";
                    predicted_value = 0;
                    if(tweet_sentiment.equals("0"))
                        context.getCounter(Global_Counters.TRUE_NEGATIVE).increment(1);
                    else
                        context.getCounter(Global_Counters.FALSE_NEGATIVE).increment(1);
                }

                context.getCounter(Global_Counters.TWEETS_PROCESSED).increment(1);

                // Output: tweet_id, text, true_label, predicted_label, timestamp
                String output = String.format("%s\t%s\t%s\t%s\t%d", 
                    tweet_id, tweet_text, tweet_sentiment, predicted_value, timestamp);
                
                context.write(new Text(tweet_id), new Text(output));

            } catch (Exception e) {
                System.err.println("Error processing tweet: " + line);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 3) {
            System.err.println("Usage: PredictSentiment <model_dir> <test_data_dir> <output_dir>");
            System.err.println("  model_dir:      HDFS path to trained model");
            System.err.println("  test_data_dir:  HDFS path to test data (streaming partitions)");
            System.err.println("  output_dir:     HDFS path for predictions");
            System.exit(1);
        }

        String model_path = args[0];
        String test_paths = args[1];  // Can be comma-separated paths
        Path output_dir = new Path(args[2]);

        Configuration conf = new Configuration();
        conf.set("model.path", model_path);
        
        FileSystem fs = FileSystem.get(conf);
        
        // Verify model exists
        Path model_dir_path = new Path(model_path);
        if(!fs.exists(model_dir_path)) {
            System.err.println("Error: Model not found at " + model_path);
            System.err.println("Train a model first: hadoop jar kafka_hadoop_nb.jar TrainModel <training_data> <model_output>");
            System.exit(1);
        }
        
        // Clean previous output
        if(fs.exists(output_dir)) {
            fs.delete(output_dir, true);
        }

        System.out.println("========================================");
        System.out.println("Sentiment Prediction (Real-time)");
        System.out.println("========================================");
        System.out.println("Model:      " + model_path);
        System.out.println("Test data:  " + test_paths);
        System.out.println("Output:     " + output_dir);
        System.out.println("========================================");

        long start_time = System.nanoTime();

        Job predict_job = Job.getInstance(conf, "Predict Sentiment");
        predict_job.setJarByClass(PredictSentiment.class);
        predict_job.setMapperClass(Map_Predict.class);
        predict_job.setNumReduceTasks(0);  // Map-only job
        predict_job.setOutputKeyClass(Text.class);
        predict_job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPaths(predict_job, test_paths);  // Support comma-separated paths
        TextOutputFormat.setOutputPath(predict_job, output_dir);
        
        boolean success = predict_job.waitForCompletion(true);

        if(!success) {
            System.err.println("Prediction job failed!");
            System.exit(1);
        }

        long execution_time = (System.nanoTime() - start_time) / 1000000000L;

        // Calculate metrics
        int tweets_processed = Math.toIntExact(predict_job.getCounters().findCounter(Global_Counters.TWEETS_PROCESSED).getValue());
        int tp = Math.toIntExact(predict_job.getCounters().findCounter(Global_Counters.TRUE_POSITIVE).getValue());
        int fp = Math.toIntExact(predict_job.getCounters().findCounter(Global_Counters.FALSE_POSITIVE).getValue());
        int tn = Math.toIntExact(predict_job.getCounters().findCounter(Global_Counters.TRUE_NEGATIVE).getValue());
        int fn = Math.toIntExact(predict_job.getCounters().findCounter(Global_Counters.FALSE_NEGATIVE).getValue());

        double accuracy = tweets_processed > 0 ? ((double) (tp + tn)) / tweets_processed : 0.0;
        double precision = (tp + fp) > 0 ? ((double) tp) / (tp + fp) : 0.0;
        double recall = (tp + fn) > 0 ? ((double) tp) / (tp + fn) : 0.0;
        double f1_score = (precision + recall) > 0 ? 2 * (precision * recall) / (precision + recall) : 0.0;

        System.out.println("\n========================================");
        System.out.println("✓ Prediction Complete!");
        System.out.println("========================================");
        System.out.println("Tweets processed: " + tweets_processed);
        System.out.println("Execution time:   " + execution_time + " seconds");
        System.out.println("\nCONFUSION MATRIX:");
        System.out.printf("                Predicted\n");
        System.out.printf("               Pos    Neg\n");
        System.out.printf("Actual Pos   [%5d] [%5d]\n", tp, fn);
        System.out.printf("       Neg   [%5d] [%5d]\n\n", fp, tn);
        System.out.printf("Accuracy:    %.4f\n", accuracy);
        System.out.printf("Precision:   %.4f\n", precision);
        System.out.printf("Recall:      %.4f\n", recall);
        System.out.printf("F1-Score:    %.4f\n", f1_score);
        System.out.println("\nPredictions saved to: " + output_dir);
        System.out.println("========================================");

        // Write to MongoDB
        try {
            MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
            MongoDatabase database = mongoClient.getDatabase("sentiment_analysis");
            MongoCollection<Document> metricsCollection = database.getCollection("hadoop_prediction_batches");
            
            String batch_id = "predict-" + new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
            
            Document metricsDoc = new Document()
                .append("batch_id", batch_id)
                .append("model", "naive_bayes_hadoop_pretrained")
                .append("model_path", model_path)
                .append("accuracy", accuracy)
                .append("precision", precision)
                .append("recall", recall)
                .append("f1_score", f1_score)
                .append("tweets_processed", tweets_processed)
                .append("execution_time_seconds", execution_time)
                .append("timestamp", new Date())
                .append("confusion_matrix", new Document()
                    .append("true_positive", tp)
                    .append("false_positive", fp)
                    .append("true_negative", tn)
                    .append("false_negative", fn));
            
            metricsCollection.insertOne(metricsDoc);
            System.out.println("\n✓ Batch metrics written to MongoDB (batch_id: " + batch_id + ")");
            
            mongoClient.close();
        } catch (Exception e) {
            System.err.println("\n⚠ Warning: Could not write metrics to MongoDB: " + e.getMessage());
        }
    }
}
