#!/usr/bin/env python3
"""
Enhanced Kafka Producer for Sentiment Analysis Pipeline
Includes preprocessing logic to match preprocess_sentiment_data.py:
- Convert sentiment: "0" = negative, "4" → "1" = positive
- Remove neutral tweets ("2")
- Clean and format data for streaming
"""

import csv
import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TweetProducerEnhanced:
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize Kafka Producer with preprocessing capabilities"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10,
            acks='all',
            retries=3
        )
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'sent_count': 0,
            'skipped_neutral': 0,
            'skipped_invalid': 0,
            'positive_count': 0,
            'negative_count': 0,
            'test_count': 0,
            'error_count': 0
        }
        
        logger.info(f"Enhanced Kafka Producer initialized: {bootstrap_servers}")
    
    def preprocess_sentiment(self, sentiment_str):
        """
        Preprocess sentiment label matching preprocess_sentiment_data.py
        
        Supports both formats:
        - Sentiment140: "0" (negative), "4" (positive), "2" (neutral - skip)
        - TweetEval/Simple: "0" (negative), "1" (positive)
        
        Args:
            sentiment_str: Original sentiment string ("0", "1", "2", "4", etc.)
        
        Returns:
            tuple: (processed_sentiment, should_skip)
                   processed_sentiment: "0" or "1"
                   should_skip: True if should be filtered out
        """
        sentiment = sentiment_str.strip('"').strip()
        
        if sentiment == '0':
            # Negative stays 0
            return '0', False
        elif sentiment == '1':
            # Positive (already in correct format)
            return '1', False
        elif sentiment == '4':
            # Positive (Sentiment140 format) becomes 1
            return '1', False
        elif sentiment == '2':
            # Skip neutral
            return None, True
        else:
            # Unknown sentiment, skip
            return None, True
    
    def clean_tweet_text(self, text):
        """
        Clean tweet text - basic cleaning
        More intensive cleaning done by consumers
        """
        # Remove quotes
        text = text.strip('"').strip()
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def parse_csv_row(self, row):
        """
        Parse CSV row from different formats:
        
        1. Sentiment140 format: Sentiment, ID, Date, Query, User, Text (6 columns)
        2. Simple format: ItemID, Sentiment, SentimentSource, SentimentText (4 columns)
        
        Returns: dict with ItemID, Sentiment, SentimentSource, Text, timestamp or None if should skip
        """
        try:
            if not row or len(row) < 4:
                return None, "invalid_row"
            
            # Detect format by number of columns
            if len(row) >= 6:
                # Sentiment140 format: Sentiment, ID, Date, Query, User, Text
                original_sentiment = row[0].strip('"')
                tweet_id = row[1].strip('"')
                tweet_text = row[5].strip('"')
                sentiment_source = 'Sentiment140'
            elif len(row) >= 4:
                # Simple format: ItemID, Sentiment, SentimentSource, SentimentText
                tweet_id = row[0].strip('"')
                original_sentiment = row[1].strip('"')
                sentiment_source = row[2].strip('"') if len(row) > 2 else 'Unknown'
                tweet_text = row[3].strip('"') if len(row) > 3 else ''
            else:
                return None, "invalid_row"
            
            # Preprocess sentiment (matching preprocess_sentiment_data.py)
            processed_sentiment, should_skip = self.preprocess_sentiment(original_sentiment)
            
            if should_skip:
                if original_sentiment == '2':
                    return None, "neutral"
                else:
                    return None, "invalid_sentiment"
            
            # Clean text
            cleaned_text = self.clean_tweet_text(tweet_text)
            
            if not cleaned_text or len(cleaned_text) < 1:
                return None, "empty_text"
            
            return {
                'ItemID': tweet_id,
                'Sentiment': processed_sentiment,
                'SentimentSource': sentiment_source,
                'Text': cleaned_text,
                'timestamp': int(time.time() * 1000)
            }, None
            
        except Exception as e:
            logger.debug(f"Error parsing row: {e}")
            return None, "parse_error"
    
    def delivery_success_callback(self, record_metadata):
        """Callback for successful message delivery"""
        logger.debug(
            f'Message delivered to {record_metadata.topic} '
            f'[{record_metadata.partition}] at offset {record_metadata.offset}'
        )
    
    def delivery_error_callback(self, exception):
        """Callback for failed message delivery"""
        logger.error(f'Message delivery failed: {exception}')
        self.stats['error_count'] += 1
    
    def _process_tweet_row(self, row, rate):
        """
        Process a single tweet row: parse, validate, and send to Kafka
        
        Returns: True if successfully sent, False otherwise
        """
        self.stats['total_processed'] += 1
        
        # Parse and preprocess row
        result, skip_reason = self.parse_csv_row(row)
        
        if result is None:
            # Track why it was skipped
            if skip_reason == "neutral":
                self.stats['skipped_neutral'] += 1
            elif skip_reason in ["invalid_sentiment", "invalid_row", "empty_text"]:
                self.stats['skipped_invalid'] += 1
            elif skip_reason == "parse_error":
                self.stats['error_count'] += 1
            return False
        
        tweet_data = result
        
        # Track sentiment counts
        if tweet_data['Sentiment'] == '0':
            self.stats['negative_count'] += 1
        elif tweet_data['Sentiment'] == '1':
            self.stats['positive_count'] += 1
        
        # Publish to testing topic only (pretrained model)
        target_topic = 'tweets-testing'
        self.stats['test_count'] += 1
        
        # Use ItemID as key for partitioning
        key = tweet_data['ItemID']
        
        try:
            # Simulate Twitter API call delay (100-200 ms)
            api_delay = random.uniform(0.1, 0.2)  # 100-200 ms in seconds
            time.sleep(api_delay)
            
            # Send to testing topic
            future_testing = self.producer.send(
                target_topic,
                key=key,
                value=tweet_data
            )
            future_testing.add_callback(self.delivery_success_callback)
            future_testing.add_errback(self.delivery_error_callback)
            
            self.stats['sent_count'] += 1
            
            # Log progress
            if self.stats['sent_count'] % 1000 == 0:
                self._log_progress()
            
            # Control rate (additional delay if needed)
            time.sleep(1.0 / rate)
            
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self.stats['error_count'] += 1
            return False
    
    def stream_from_csv(self, csv_file, rate=100):
        """
        Stream tweets from CSV file to Kafka topics with preprocessing
        
        Args:
            csv_file: Path to CSV file
            rate: Messages per second
        """
        logger.info(f"Starting to stream from {csv_file}")
        logger.info(f"Rate: {rate} msgs/sec")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f)
                
                # Check if first row is a header (skip it if it looks like a header)
                first_row = next(reader, None)
                if first_row and len(first_row) > 0:
                    first_cell = first_row[0].strip('"').strip().lower()
                    # If first cell looks like a header (contains "itemid", "sentiment", etc.), skip it
                    if first_cell in ['itemid', 'sentiment', 'id', 'label']:
                        logger.info("Detected CSV header row, skipping...")
                    else:
                        # Not a header, process this row
                        self._process_tweet_row(first_row, rate)
                
                # Process remaining rows
                for row_num, row in enumerate(reader, 2):
                    self._process_tweet_row(row, rate)
                
        except FileNotFoundError:
            logger.error(f"File not found: {csv_file}")
            return
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return
        finally:
            # Flush and close producer
            self.producer.flush()
            
            # Final statistics
            elapsed_time = time.time() - start_time
            self._log_final_stats(elapsed_time)
    
    
    def _log_progress(self):
        """Log current progress"""
        logger.info(
            f"Progress: Sent {self.stats['sent_count']:,} | "
            f"Pos: {self.stats['positive_count']:,} | "
            f"Neg: {self.stats['negative_count']:,} | "
            f"Skipped: {self.stats['skipped_neutral']:,} neutral, "
            f"{self.stats['skipped_invalid']:,} invalid"
        )
    
    def _log_final_stats(self, elapsed_time):
        """Log final statistics"""
        logger.info("\n" + "=" * 60)
        logger.info("STREAMING COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Total rows processed:    {self.stats['total_processed']:,}")
        logger.info(f"✓ Successfully sent:     {self.stats['sent_count']:,}")
        logger.info(f"  - Positive (1):        {self.stats['positive_count']:,}")
        logger.info(f"  - Negative (0):        {self.stats['negative_count']:,}")
        logger.info(f"  - Testing set:         {self.stats['test_count']:,}")
        logger.info(f"✗ Skipped neutral (2):   {self.stats['skipped_neutral']:,}")
        logger.info(f"✗ Skipped invalid:       {self.stats['skipped_invalid']:,}")
        logger.info(f"✗ Errors:                {self.stats['error_count']:,}")
        logger.info(f"\nElapsed time:            {elapsed_time:.2f} seconds")
        
        if elapsed_time > 0:
            logger.info(f"Average rate:            {self.stats['sent_count']/elapsed_time:.2f} msgs/sec")
        
        logger.info("=" * 60)
    
    def close(self):
        """Close producer"""
        self.producer.close()
        logger.info("Producer closed")


def main():
    parser = argparse.ArgumentParser(
        description='Kafka Producer for Tweet Sentiment Analysis - Testing Only (Pretrained Model)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream with default CSV file
  python3 tweet_producer.py

  # Stream with custom CSV file
  python3 tweet_producer.py --csv-file ../data/raws/tweeteval_sentiment_test.csv --rate 50
        """
    )
    parser.add_argument(
        '--csv-file',
        default='../data/raws/testdata.manual.2009.06.14.csv',
        help='Path to CSV file with tweets (Sentiment140 format). Default: data/raws/testdata.manual.2009.06.14.csv'
    )
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--rate',
        type=int,
        default=100,
        help='Messages per second (default: 100)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate arguments
    if args.rate <= 0:
        parser.error("--rate must be positive")
    
    logger.info("=" * 60)
    logger.info("KAFKA PRODUCER - SENTIMENT ANALYSIS (TESTING ONLY)")
    logger.info("=" * 60)
    logger.info(f"Input file:       {args.csv_file}")
    logger.info(f"Kafka servers:    {args.bootstrap_servers}")
    logger.info(f"Rate:             {args.rate} msgs/sec")
    logger.info(f"Target topic:     tweets-testing")
    logger.info("=" * 60)
    logger.info("Preprocessing: Convert sentiment labels (0→0, 4→1, skip 2)")
    logger.info("=" * 60 + "\n")
    
    # Create producer
    producer = TweetProducerEnhanced(bootstrap_servers=args.bootstrap_servers)
    
    try:
        producer.stream_from_csv(args.csv_file, args.rate)
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        producer.close()


if __name__ == '__main__':
    main()
