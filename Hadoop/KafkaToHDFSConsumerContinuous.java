import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Continuous Kafka to HDFS Consumer with Offset Tracking
 * 
 * Features:
 * - Automatic offset tracking (resumes from last position)
 * - Time-based partitioning (hourly/minutely directories)
 * - Continuous consumption mode
 * - Graceful shutdown on SIGTERM
 * 
 * Usage:
 *   hadoop jar kafka_to_hdfs_consumer.jar KafkaToHDFSConsumerContinuous \
 *     <topic> <hdfs_base_dir> <partition_mode> [max_batches]
 * 
 * partition_mode: "hourly" or "minutely"
 * max_batches: optional, for testing (default: unlimited)
 */
public class KafkaToHDFSConsumerContinuous {
    
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String HDFS_URI = "hdfs://localhost:9000";
    private static final int BATCH_SIZE = 1000;
    private static final int POLL_TIMEOUT_MS = 10000;
    private static volatile boolean running = true;
    
    private static DateTimeFormatter hourlyFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    private static DateTimeFormatter minutelyFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm");
    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: KafkaToHDFSConsumerContinuous <topic> <hdfs_base_dir> <partition_mode> [max_batches]");
            System.err.println("  topic:          Kafka topic to consume");
            System.err.println("  hdfs_base_dir:  Base HDFS directory (e.g., /user/hadoop/kafka_data/tweets-training)");
            System.err.println("  partition_mode: 'hourly' or 'minutely'");
            System.err.println("  max_batches:    Optional - max batches to process (for testing)");
            System.err.println("");
            System.err.println("Example:");
            System.err.println("  hadoop jar kafka_to_hdfs_consumer.jar KafkaToHDFSConsumerContinuous \\");
            System.err.println("    tweets-training /user/hadoop/kafka_data/tweets-training hourly");
            System.exit(1);
        }
        
        String topic = args[0];
        String hdfsBaseDir = args[1];
        String partitionMode = args[2];
        int maxBatches = args.length > 3 ? Integer.parseInt(args[3]) : -1; // -1 = unlimited
        
        if (!partitionMode.equals("hourly") && !partitionMode.equals("minutely")) {
            System.err.println("Error: partition_mode must be 'hourly' or 'minutely'");
            System.exit(1);
        }
        
        System.out.println("========================================");
        System.out.println("Continuous Kafka to HDFS Consumer");
        System.out.println("========================================");
        System.out.println("Topic:           " + topic);
        System.out.println("HDFS Base Dir:   " + hdfsBaseDir);
        System.out.println("Partition Mode:  " + partitionMode);
        System.out.println("Max Batches:     " + (maxBatches == -1 ? "Unlimited" : maxBatches));
        System.out.println("Offset Tracking: Enabled (auto-commit)");
        System.out.println("========================================\n");
        
        // Register shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[SHUTDOWN] Received shutdown signal. Stopping consumer...");
            running = false;
        }));
        
        // Setup Kafka Consumer with offset tracking
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hdfs-consumer-continuous-" + topic);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");  // Automatic offset commit
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        
        // Setup HDFS
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(conf);
        
        // Create base directory
        Path basePath = new Path(hdfsBaseDir);
        if (!fs.exists(basePath)) {
            fs.mkdirs(basePath);
            System.out.println("✓ Created base HDFS directory: " + hdfsBaseDir);
        }
        
        // Statistics
        int totalMessages = 0;
        int totalBatches = 0;
        long startTime = System.currentTimeMillis();
        String currentPartition = "";
        List<String> batch = new ArrayList<>();
        int batchCounter = 1;
        
        System.out.println("\n[READY] Consuming messages from Kafka (Ctrl+C to stop)...\n");
        
        try {
            while (running && (maxBatches == -1 || totalBatches < maxBatches)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                
                if (records.isEmpty()) {
                    // No messages, wait and continue
                    if (totalMessages > 0 && totalMessages % 10000 == 0) {
                        System.out.println("[IDLE] Waiting for messages... (Total processed: " + totalMessages + ")");
                    }
                    continue;
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    batch.add(message);
                    totalMessages++;
                    
                    // Check if we need to write batch
                    if (batch.size() >= BATCH_SIZE) {
                        // Determine time-based partition
                        String partition = getPartition(partitionMode);
                        
                        // If partition changed, announce it
                        if (!partition.equals(currentPartition)) {
                            if (!currentPartition.isEmpty()) {
                                System.out.println("\n[PARTITION CHANGE] " + currentPartition + " → " + partition);
                            }
                            currentPartition = partition;
                            batchCounter = 1; // Reset batch counter for new partition
                        }
                        
                        // Write batch to partition directory
                        writeBatchToHDFS(fs, basePath, partition, batchCounter, batch);
                        
                        totalBatches++;
                        
                        // Progress update
                        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                        double rate = elapsed > 0 ? totalMessages / (double) elapsed : 0;
                        
                        System.out.printf("[%s] Batch %d: %d msgs (Total: %,d, Rate: %.1f msg/s)%n", 
                            partition, batchCounter, batch.size(), totalMessages, rate);
                        
                        batch.clear();
                        batchCounter++;
                        
                        // Periodic summary
                        if (totalBatches % 10 == 0) {
                            printSummary(totalMessages, totalBatches, startTime);
                        }
                    }
                }
            }
            
            // Write remaining messages
            if (!batch.isEmpty()) {
                String partition = getPartition(partitionMode);
                writeBatchToHDFS(fs, basePath, partition, batchCounter, batch);
                totalBatches++;
                System.out.printf("[%s] Final batch %d: %d messages%n", partition, batchCounter, batch.size());
            }
            
        } catch (Exception e) {
            System.err.println("\n[ERROR] Exception occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("\n[CLEANUP] Closing consumer...");
            consumer.close();
            fs.close();
        }
        
        long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
        
        System.out.println("\n========================================");
        System.out.println("✓ Consumer Stopped");
        System.out.println("========================================");
        System.out.println("Total Messages:  " + String.format("%,d", totalMessages));
        System.out.println("Total Batches:   " + totalBatches);
        System.out.println("Time Elapsed:    " + formatDuration(elapsedTime));
        System.out.println("Average Rate:    " + String.format("%.1f", totalMessages / Math.max(1, (double) elapsedTime)) + " msgs/sec");
        System.out.println("HDFS Location:   " + hdfsBaseDir);
        System.out.println("========================================");
    }
    
    private static String getPartition(String mode) {
        LocalDateTime now = LocalDateTime.now();
        if (mode.equals("hourly")) {
            return now.format(hourlyFormatter);
        } else {
            return now.format(minutelyFormatter);
        }
    }
    
    private static void writeBatchToHDFS(FileSystem fs, Path baseDir, String partition, int batchNum, List<String> messages) 
            throws IOException {
        // Create partition directory
        Path partitionDir = new Path(baseDir, partition);
        if (!fs.exists(partitionDir)) {
            fs.mkdirs(partitionDir);
        }
        
        // Write batch file
        String filename = String.format("batch-%05d.json", batchNum);
        Path filePath = new Path(partitionDir, filename);
        
        FSDataOutputStream out = fs.create(filePath, true);
        
        for (String message : messages) {
            out.write((message + "\n").getBytes("UTF-8"));
        }
        
        out.close();
    }
    
    private static void printSummary(int totalMessages, int totalBatches, long startTime) {
        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        double rate = elapsed > 0 ? totalMessages / (double) elapsed : 0;
        
        System.out.println("\n--- Summary ---");
        System.out.printf("  Messages: %,d | Batches: %d | Runtime: %s | Rate: %.1f msg/s%n",
            totalMessages, totalBatches, formatDuration(elapsed), rate);
        System.out.println("---------------\n");
    }
    
    private static String formatDuration(long seconds) {
        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;
        
        if (hours > 0) {
            return String.format("%dh %dm %ds", hours, minutes, secs);
        } else if (minutes > 0) {
            return String.format("%dm %ds", minutes, secs);
        } else {
            return String.format("%ds", secs);
        }
    }
}
