import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.*;
import java.util.*;

/**
 * Training-Only MapReduce Job
 * 
 * Trains Naive Bayes model ONCE and saves to HDFS
 * Usage: hadoop jar kafka_hadoop_nb.jar TrainModel <training_data_dir> <model_output_dir>
 */
public class TrainModel {
    
    public static enum Global_Counters {
        TWEETS_SIZE,
        POS_TWEETS_SIZE,
        NEG_TWEETS_SIZE,
        POS_WORDS_SIZE,
        NEG_WORDS_SIZE,
        FEATURES_SIZE
    }

    /* 
     * Training Mapper
     * Input:  <byte_offset, csv_line>
     * CSV Format: tweet_id,sentiment,source,tweet_text
     * Output: <word, sentiment>
     */
    public static class Map_Training extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            if(line.isEmpty()) 
                return;

            try {
                // Parse CSV: tweet_id,sentiment,source,tweet_text
                // Split only on first 3 commas to handle commas in tweet text
                String[] parts = line.split(",", 4);
                
                if(parts.length < 4) {
                    return; // Skip malformed lines
                }
                
                String tweet_id = parts[0];
                String tweet_sentiment = parts[1];
                String source = parts[2];
                String tweet_text = parts[3];
                
                context.getCounter(Global_Counters.TWEETS_SIZE).increment(1);

                // Clean text
                tweet_text = tweet_text.replaceAll("(?i)(https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|www\\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9]+\\.[^\\s]{2,}|www\\.[a-zA-Z0-9]+\\.[^\\s]{2,})", "")
                                    .replaceAll("(#|@|&).*?\\w+", "")
                                    .replaceAll("\\d+", "")
                                    .replaceAll("[^a-zA-Z ]", " ")
                                    .toLowerCase()
                                    .trim()
                                    .replaceAll("\\s+", " ");

                String sentiment_label = "POSITIVE";

                if(tweet_sentiment.equals("1")) {
                    context.getCounter(Global_Counters.POS_TWEETS_SIZE).increment(1);
                    context.getCounter(Global_Counters.POS_WORDS_SIZE).increment(tweet_text.split("\\s+").length);
                } else {
                    context.getCounter(Global_Counters.NEG_TWEETS_SIZE).increment(1);
                    context.getCounter(Global_Counters.NEG_WORDS_SIZE).increment(tweet_text.split("\\s+").length);
                    sentiment_label = "NEGATIVE";
                }

                if(tweet_text != null && !tweet_text.trim().isEmpty()) {
                    String[] tweet_words = tweet_text.split(" ");
                    for(String word : tweet_words) {
                        if(!word.isEmpty())
                            context.write(new Text(word), new Text(sentiment_label));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error parsing CSV: " + line);
            }
        }
    }

    /* 
     * Training Reducer
     * Input:  <word, sentiment>
     * Output: <word, pos_wordcount@neg_wordcount>
     */
    public static class Reduce_Training extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.getCounter(Global_Counters.FEATURES_SIZE).increment(1);

            int positive_counter = 0;
            int negative_counter = 0;

            for(Text value : values) {
                String sentiment = value.toString();
                if(sentiment.equals("POSITIVE"))
                    positive_counter++;
                else
                    negative_counter++;
            }

            context.write(key, new Text(String.valueOf(positive_counter) + "@" + String.valueOf(negative_counter)));
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            System.err.println("Usage: TrainModel <training_data_dir> <model_output_dir>");
            System.err.println("  training_data_dir: HDFS path to training data (can be partitioned)");
            System.err.println("  model_output_dir:  HDFS path to save model (e.g., /user/hadoop/models/nb_model)");
            System.exit(1);
        }

        Path input_dir = new Path(args[0]);
        Path model_dir = new Path(args[1]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        // Clean previous model
        if(fs.exists(model_dir)) {
            System.out.println("Removing existing model: " + model_dir);
            fs.delete(model_dir, true);
        }

        System.out.println("========================================");
        System.out.println("Training Naive Bayes Model");
        System.out.println("========================================");
        System.out.println("Training data: " + input_dir);
        System.out.println("Model output:  " + model_dir);
        System.out.println("========================================");

        long start_time = System.nanoTime();

        Job training_job = Job.getInstance(conf, "Train NB Model");
        training_job.setJarByClass(TrainModel.class);
        training_job.setMapperClass(Map_Training.class);
        training_job.setReducerClass(Reduce_Training.class);
        training_job.setNumReduceTasks(3);
        training_job.setMapOutputKeyClass(Text.class);
        training_job.setMapOutputValueClass(Text.class);
        training_job.setOutputKeyClass(Text.class);
        training_job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(training_job, input_dir);
        TextOutputFormat.setOutputPath(training_job, model_dir);
        
        boolean success = training_job.waitForCompletion(true);

        if(!success) {
            System.err.println("Training job failed!");
            System.exit(1);
        }

        long execution_time = (System.nanoTime() - start_time) / 1000000000L;

        // Extract and save counters
        int tweets_size = Math.toIntExact(training_job.getCounters().findCounter(Global_Counters.TWEETS_SIZE).getValue());
        int pos_tweets_size = Math.toIntExact(training_job.getCounters().findCounter(Global_Counters.POS_TWEETS_SIZE).getValue());
        int neg_tweets_size = Math.toIntExact(training_job.getCounters().findCounter(Global_Counters.NEG_TWEETS_SIZE).getValue());
        int pos_words_size = Math.toIntExact(training_job.getCounters().findCounter(Global_Counters.POS_WORDS_SIZE).getValue());
        int neg_words_size = Math.toIntExact(training_job.getCounters().findCounter(Global_Counters.NEG_WORDS_SIZE).getValue());
        int features_size = Math.toIntExact(training_job.getCounters().findCounter(Global_Counters.FEATURES_SIZE).getValue());

        // Save metadata
        Path metadata_path = new Path(model_dir, "_metadata.txt");
        FSDataOutputStream out = fs.create(metadata_path, true);
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(out));
        
        writer.println("tweets_size=" + tweets_size);
        writer.println("pos_tweets_size=" + pos_tweets_size);
        writer.println("neg_tweets_size=" + neg_tweets_size);
        writer.println("pos_words_size=" + pos_words_size);
        writer.println("neg_words_size=" + neg_words_size);
        writer.println("features_size=" + features_size);
        writer.println("training_time_seconds=" + execution_time);
        writer.println("created_at=" + new Date().toString());
        
        writer.close();
        out.close();

        System.out.println("\n========================================");
        System.out.println("✓ Training Complete!");
        System.out.println("========================================");
        System.out.println("Total tweets:     " + tweets_size);
        System.out.println("Positive tweets:  " + pos_tweets_size);
        System.out.println("Negative tweets:  " + neg_tweets_size);
        System.out.println("Vocabulary size:  " + features_size);
        System.out.println("Training time:    " + execution_time + " seconds");
        System.out.println("\nModel saved to:   " + model_dir);
        System.out.println("Metadata saved:   " + metadata_path);
        System.out.println("========================================");
    }
}
