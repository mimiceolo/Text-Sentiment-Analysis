#!/usr/bin/env python3
"""
Preprocess Sentiment140 dataset to convert sentiment labels:
- Original: "0" = negative, "4" = positive, "2" = neutral
- Output: "0" = negative, "1" = positive (remove neutral)
"""

import sys
import csv

def preprocess_sentiment_file(input_file, output_file):
    """Convert sentiment labels from 0/2/4 to 0/1 format"""
    print(f"Processing {input_file}...")
    
    processed_count = 0
    skipped_count = 0
    positive_count = 0
    negative_count = 0
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONE, escapechar='\\')
        
        for row in reader:
            if len(row) < 6:
                continue
            
            sentiment = row[0].strip('"')
            
            # Convert sentiment labels
            if sentiment == '0':
                # Negative stays 0
                new_sentiment = '0'
                negative_count += 1
            elif sentiment == '4':
                # Positive becomes 1
                new_sentiment = '1'
                positive_count += 1
            elif sentiment == '2':
                # Skip neutral
                skipped_count += 1
                continue
            else:
                # Unknown sentiment, skip
                skipped_count += 1
                continue
            
            # Write modified row
            tweet_id = row[1].strip('"')
            tweet_text = row[5].strip('"')
            outfile.write(f'{tweet_id},{new_sentiment},Sentiment140,{tweet_text}\n')
            processed_count += 1
    
    print(f"✓ Processed {processed_count} records")
    print(f"  - Negative (0): {negative_count}")
    print(f"  - Positive (1): {positive_count}")
    print(f"  - Skipped: {skipped_count}")
    print(f"✓ Output written to {output_file}\n")

if __name__ == "__main__":
    # Process training data
    preprocess_sentiment_file(
        'data/raws/training.1600000.processed.noemoticon.csv',
        'data/preprocessed/training_processed.csv'
    )
    
    # Process test data
    preprocess_sentiment_file(
        'data/raws/testdata.manual.2009.06.14.csv',
        'data/preprocessed/test_processed.csv'
    )
    
    print("=" * 60)
    print("Data preprocessing complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Upload to HDFS:")
    print("   hadoop fs -rm -r hadoop_training hadoop_testing")
    print("   hadoop fs -mkdir hadoop_training hadoop_testing")
    print("   hadoop fs -put training_processed.csv hadoop_training/")
    print("   hadoop fs -put test_processed.csv hadoop_testing/")
    print("\n2. Run your Hadoop job")
