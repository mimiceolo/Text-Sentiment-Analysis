"""
Aggregate sentiment metrics periodically and store in MongoDB
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
import time

MONGO_URI = 'mongodb://localhost:27017/sentiment_analysis'
client = MongoClient(MONGO_URI)
db = client.sentiment_analysis

def aggregate_hourly_metrics():
    """Aggregate metrics by hour"""
    one_hour_ago = datetime.now() - timedelta(hours=1)

    pipeline = [
        {'$match': {'timestamp': {'$gte': one_hour_ago}}},
        {'$group': {
            '_id': {
                'model': '$model',
                'hour': {'$dateToString': {'format': '%Y-%m-%d-%H', 'date': '$timestamp'}}
            },
            'total_predictions': {'$sum': 1},
            'positive_predictions': {
                '$sum': {'$cond': [{'$eq': ['$predicted_label', 1]}, 1, 0]}
            },
            'negative_predictions': {
                '$sum': {'$cond': [{'$eq': ['$predicted_label', 0]}, 1, 0]}
            },
            'correct_predictions': {
                '$sum': {'$cond': ['$correct', 1, 0]}
            }
        }},
        {'$project': {
            'model': '$_id.model',
            'hour': '$_id.hour',
            'total_predictions': 1,
            'positive_predictions': 1,
            'negative_predictions': 1,
            'correct_predictions': 1,
            'accuracy': {'$divide': ['$correct_predictions', '$total_predictions']},
            'sentiment_ratio': {'$divide': ['$positive_predictions', '$total_predictions']},
            'timestamp': datetime.now()
        }}
    ]

    results = list(db.predictions.aggregate(pipeline))

    if results:
        for result in results:
            result.pop('_id', None)
            db.sentiment_trends.update_one(
                {'model': result['model'], 'hour': result['hour']},
                {'$set': result},
                upsert=True
            )
        print(f"Aggregated {len(results)} hourly metrics")

def main():
    print("Starting metrics aggregation service...")

    while True:
        try:
            aggregate_hourly_metrics()
        except Exception as e:
            print(f"Error aggregating metrics: {e}")

        # Run every 5 minutes
        time.sleep(300)

if __name__ == '__main__':
    main()

