from datasets import load_dataset
import pandas as pd

dataset = load_dataset("tweet_eval", "sentiment")

def to_csv(split, filename):
    rows = []
    idx = 1
    for item in split:
        if item["label"] == 1:  # neutral
            continue
        rows.append({
            "ItemID": idx,
            "Sentiment": 1 if item["label"] == 2 else 0,
            "SentimentSource": "TweetEval",
            "SentimentText": item["text"]
        })
        idx += 1

    pd.DataFrame(rows).to_csv(filename, index=False)

to_csv(dataset["test"], "tweeteval_sentiment_test.csv")



