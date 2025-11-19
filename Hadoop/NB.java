import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Counters;

import java.io.*;
import java.io.IOException;
import java.util.*;
import java.nio.charset.StandardCharsets;

public class NB {
    public static enum Global_Counters
    {
        TWEETS_SIZE,
        POS_TWEETS_SIZE,
        NEG_TWEETS_SIZE,
        POS_WORDS_SIZE,
        NEG_WORDS_SIZE,
        FEATURES_SIZE,
        TRUE_POSITIVE,
        FALSE_POSITIVE,
        TRUE_NEGATIVE,
        FALSE_NEGATIVE
    }

    /* input:  <byte_offset, line_of_tweet>
     * output: <word, sentiment>
     */
    public static class Map_Training extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            context.getCounter(Global_Counters.TWEETS_SIZE).increment(1);

            String line = value.toString();
            String[] columns = line.split(",");

            // if the columns are more than 4, that means the text of the post had commas inside,
            // so stitch the last columns together to form the full text of the tweet
            if(columns.length > 4)
            {
                for(int i=4; i<columns.length; i++)
                    columns[3] += columns[i];
            }

            String tweet_sentiment = columns[1];
            String tweet_text = columns[3];

            // clean the text of the tweet from links...
            tweet_text = tweet_text.replaceAll("(?i)(https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|www\\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\\.[^\\s]{2,}|https?:\\/\\/(?:www\\.|(?!www))[a-zA-Z0-9]+\\.[^\\s]{2,}|www\\.[a-zA-Z0-9]+\\.[^\\s]{2,})", "")
                    .replaceAll("(#|@|&).*?\\w+", "")   // mentions, hashtags, special characters...
                    .replaceAll("\\d+", "")             // numbers...
                    .replaceAll("[^a-zA-Z ]", " ")      // punctuation...
                    .toLowerCase()                      // turn every character left to lowercase...
                    .trim()                             // trim the spaces before & after the whole string...
                    .replaceAll("\\s+", " ");           // and get rid of double spaces

            String sentiment_label = "POSITIVE";

            if(tweet_sentiment.equals("1"))
            {
                context.getCounter(Global_Counters.POS_TWEETS_SIZE).increment(1);
                context.getCounter(Global_Counters.POS_WORDS_SIZE).increment(tweet_text.split("\\s+").length);
            }
            else
            {
                context.getCounter(Global_Counters.NEG_TWEETS_SIZE).increment(1);
                context.getCounter(Global_Counters.NEG_WORDS_SIZE).increment(tweet_text.split("\\s+").length);
                sentiment_label = "NEGATIVE";
            }


            if(tweet_text != null && !tweet_text.trim().isEmpty())
            {
                String[] tweet_words = tweet_text.split(" ");

                for(String word : tweet_words)
                    context.write(new Text(word), new Text(sentiment_label));
            }
        }
    }
}