#!/usr/bin/env python3
"""
Test script to verify Kafka producer preprocessing matches preprocess_sentiment_data.py
"""

import sys
import csv

def test_preprocessing():
    """Test that sentiment preprocessing works correctly"""
    
    print("=" * 60)
    print("TESTING KAFKA PRODUCER PREPROCESSING")
    print("=" * 60)
    
    # Test cases
    test_cases = [
        ("0", "0", False, "Negative should stay 0"),
        ("4", "1", False, "Positive should become 1"),
        ("2", None, True, "Neutral should be skipped"),
        ("3", None, True, "Invalid sentiment should be skipped"),
        ('"0"', "0", False, "Quoted negative should work"),
        ('"4"', "1", False, "Quoted positive should work"),
    ]
    
    # Import the preprocessing function
    sys.path.insert(0, '.')
    from tweet_producer import TweetProducerEnhanced
    
    producer = TweetProducerEnhanced()
    
    all_passed = True
    
    for i, (input_val, expected_output, expected_skip, description) in enumerate(test_cases, 1):
        result, should_skip = producer.preprocess_sentiment(input_val)
        
        if result == expected_output and should_skip == expected_skip:
            status = "✓ PASS"
        else:
            status = "✗ FAIL"
            all_passed = False
        
        print(f"\nTest {i}: {description}")
        print(f"  Input:          {input_val}")
        print(f"  Expected:       {expected_output}, skip={expected_skip}")
        print(f"  Got:            {result}, skip={should_skip}")
        print(f"  Status:         {status}")
    
    print("\n" + "=" * 60)
    if all_passed:
        print("✓ ALL TESTS PASSED")
    else:
        print("✗ SOME TESTS FAILED")
    print("=" * 60)
    
    return all_passed


def compare_with_original(original_csv, num_samples=10):
    """Compare processing with original preprocess_sentiment_data.py"""
    
    print("\n" + "=" * 60)
    print("COMPARING WITH ORIGINAL PREPROCESSING")
    print("=" * 60)
    
    sys.path.insert(0, '.')
    from tweet_producer import TweetProducerEnhanced
    
    producer = TweetProducerEnhanced()
    
    print(f"\nSampling {num_samples} rows from {original_csv}...\n")
    
    try:
        with open(original_csv, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            
            sample_count = 0
            match_count = 0
            skip_count = 0
            
            for row in reader:
                if sample_count >= num_samples:
                    break
                
                if not row or len(row) < 6:
                    continue
                
                original_sentiment = row[0].strip('"')
                tweet_id = row[1].strip('"')
                tweet_text = row[5].strip('"')
                
                # What original script would do
                if original_sentiment == '0':
                    expected = '0'
                    should_skip = False
                elif original_sentiment == '4':
                    expected = '1'
                    should_skip = False
                elif original_sentiment == '2':
                    expected = None
                    should_skip = True
                else:
                    expected = None
                    should_skip = True
                
                # What our producer does
                result, actual_skip = producer.preprocess_sentiment(original_sentiment)
                
                # Compare
                if result == expected and actual_skip == should_skip:
                    status = "✓"
                    match_count += 1
                else:
                    status = "✗"
                
                if should_skip:
                    skip_count += 1
                    action = "SKIP"
                else:
                    action = f"KEEP (0→{expected})"
                
                print(f"{status} ID:{tweet_id[:8]:8s} Sentiment:{original_sentiment} → {action}")
                
                sample_count += 1
        
        print("\n" + "=" * 60)
        print(f"Sampled: {sample_count} rows")
        print(f"Matched: {match_count} rows")
        print(f"Skipped: {skip_count} rows (neutral/invalid)")
        
        if match_count == sample_count:
            print("✓ PREPROCESSING MATCHES ORIGINAL SCRIPT")
        else:
            print("✗ PREPROCESSING DIFFERS FROM ORIGINAL SCRIPT")
        print("=" * 60)
        
        return match_count == sample_count
        
    except FileNotFoundError:
        print(f"✗ File not found: {original_csv}")
        return False


def main():
    # Run unit tests
    tests_passed = test_preprocessing()
    
    # Compare with original preprocessing
    comparison_file = '../training.1600000.processed.noemoticon.csv'
    comparison_passed = compare_with_original(comparison_file, num_samples=20)
    
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"Unit tests:         {'✓ PASSED' if tests_passed else '✗ FAILED'}")
    print(f"Comparison test:    {'✓ PASSED' if comparison_passed else '✗ FAILED'}")
    print("=" * 60)
    
    if tests_passed and comparison_passed:
        print("\n✓ Kafka producer preprocessing is correct!")
        print("You can now safely use tweet_producer.py")
        return 0
    else:
        print("\n✗ Some tests failed. Please review the code.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
