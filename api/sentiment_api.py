#!/usr/bin/env python3
"""
Real-time Sentiment Analysis API
Provides REST endpoints for querying MongoDB predictions
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta
import os

app = Flask(__name__)
CORS(app)

# MongoDB connection
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/sentiment_analysis')
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client.sentiment_analysis
    # Test connection
    client.admin.command('ping')
    print(f"✓ Connected to MongoDB: {MONGO_URI}")
except Exception as e:
    print(f"✗ Failed to connect to MongoDB: {e}")
    print("  Please ensure MongoDB is running and credentials are correct")
    db = None


@app.route('/')
def index():
    """Root endpoint with API documentation"""
    return jsonify({
        'name': 'Sentiment Analysis API',
        'version': '1.0',
        'endpoints': {
            '/api/health': 'Health check',
            '/api/predictions/recent': 'Get recent predictions',
            '/api/predictions/stats': 'Get prediction statistics',
            '/api/metrics/batch': 'Get batch metrics',
            '/api/sentiment/trend': 'Get sentiment trend over time',
            '/api/models/compare': 'Compare model performance',
            '/api/search/tweets': 'Search tweets by text'
        }
    })


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    if db is None:
        return jsonify({'status': 'unhealthy', 'mongodb': 'disconnected'}), 500
    
    try:
        db.command('ping')
        
        # Get collection counts
        predictions_count = db.predictions.count_documents({})
        metrics_count = db.batch_metrics.count_documents({})
        hadoop_predictions_count = db.hadoop_prediction_batches.count_documents({})
        
        return jsonify({
            'status': 'healthy',
            'mongodb': 'connected',
            'collections': {
                'predictions': predictions_count,
                'batch_metrics': metrics_count,
                'hadoop_prediction_batches': hadoop_predictions_count
            },
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500


@app.route('/api/predictions/recent', methods=['GET'])
def get_recent_predictions():
    """Get recent predictions"""
    if db is None:
        return jsonify({'error': 'Database not available'}), 500
    
    limit = int(request.args.get('limit', 100))
    model = request.args.get('model', None)
    
    query = {}
    if model:
        query['model'] = model
    
    try:
        predictions = list(db.predictions.find(
            query,
            {'_id': 0}
        ).sort('timestamp', DESCENDING).limit(limit))
        
        return jsonify({
            'count': len(predictions),
            'predictions': predictions
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/predictions/stats', methods=['GET'])
def get_prediction_stats():
    """Get prediction statistics"""
    if db is None:
        return jsonify({'error': 'Database not available'}), 500
    
    model = request.args.get('model', None)
    hours = int(request.args.get('hours', 24))
    
    # Calculate time range
    time_threshold = datetime.now() - timedelta(hours=hours)
    
    match_query = {'timestamp': {'$gte': time_threshold}}
    if model:
        match_query['model'] = model
    
    # Aggregation pipeline
    pipeline = [
        {'$match': match_query},
        {'$group': {
            '_id': '$model',
            'total': {'$sum': 1},
            'correct': {'$sum': {'$cond': ['$correct', 1, 0]}},
            'positive_predictions': {
                '$sum': {'$cond': [{'$eq': ['$predicted_label', 1]}, 1, 0]}
            },
            'negative_predictions': {
                '$sum': {'$cond': [{'$eq': ['$predicted_label', 0]}, 1, 0]}
            }
        }},
        {'$project': {
            'model': '$_id',
            'total': 1,
            'correct': 1,
            'accuracy': {'$divide': ['$correct', '$total']},
            'positive_predictions': 1,
            'negative_predictions': 1,
            'positive_ratio': {'$divide': ['$positive_predictions', '$total']}
        }}
    ]
    
    try:
        stats = list(db.predictions.aggregate(pipeline))
        
        return jsonify({
            'time_range_hours': hours,
            'stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/batch', methods=['GET'])
def get_batch_metrics():
    """Get batch metrics over time from both Spark and Hadoop"""
    if db is None:
        return jsonify({'error': 'Database not available'}), 500
    
    model = request.args.get('model', None)
    limit = int(request.args.get('limit', 50))
    source = request.args.get('source', 'all')  # 'all', 'spark', 'hadoop'
    
    query = {}
    if model:
        query['model'] = model
    
    try:
        metrics = []
        
        # Get Spark metrics
        if source in ['all', 'spark']:
            spark_metrics = list(db.batch_metrics.find(
                query,
                {'_id': 0}
            ).sort('timestamp', DESCENDING).limit(limit))
            
            # Add source tag
            for metric in spark_metrics:
                metric['source'] = 'spark'
            
            metrics.extend(spark_metrics)
        
        # Get Hadoop metrics
        if source in ['all', 'hadoop']:
            hadoop_metrics = list(db.hadoop_prediction_batches.find(
                query,
                {'_id': 0}
            ).sort('timestamp', DESCENDING).limit(limit))
            
            # Add source tag
            for metric in hadoop_metrics:
                metric['source'] = 'hadoop'
            
            metrics.extend(hadoop_metrics)
        
        # Sort combined results by timestamp
        metrics.sort(key=lambda x: x.get('timestamp', datetime.min), reverse=True)
        
        return jsonify({
            'count': len(metrics),
            'metrics': metrics[:limit]  # Limit after combining
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sentiment/trend', methods=['GET'])
def get_sentiment_trend():
    """Get sentiment trend over time"""
    if db is None:
        return jsonify({'error': 'Database not available'}), 500
    
    hours = int(request.args.get('hours', 24))
    interval_minutes = int(request.args.get('interval', 60))
    model = request.args.get('model', None)
    
    time_threshold = datetime.now() - timedelta(hours=hours)
    
    match_query = {'timestamp': {'$gte': time_threshold}}
    if model:
        match_query['model'] = model
    
    # Group by time intervals
    pipeline = [
        {'$match': match_query},
        {'$group': {
            '_id': {
                'model': '$model',
                'interval': {
                    '$toDate': {
                        '$subtract': [
                            {'$toLong': '$timestamp'},
                            {'$mod': [
                                {'$toLong': '$timestamp'},
                                interval_minutes * 60 * 1000
                            ]}
                        ]
                    }
                }
            },
            'positive_count': {
                '$sum': {'$cond': [{'$eq': ['$predicted_label', 1]}, 1, 0]}
            },
            'negative_count': {
                '$sum': {'$cond': [{'$eq': ['$predicted_label', 0]}, 1, 0]}
            },
            'total_count': {'$sum': 1}
        }},
        {'$sort': {'_id.interval': 1}},
        {'$project': {
            'model': '$_id.model',
            'timestamp': '$_id.interval',
            'positive_count': 1,
            'negative_count': 1,
            'total_count': 1,
            'sentiment_ratio': {
                '$divide': ['$positive_count', '$total_count']
            }
        }}
    ]
    
    try:
        trend = list(db.predictions.aggregate(pipeline))
        
        return jsonify({
            'time_range_hours': hours,
            'interval_minutes': interval_minutes,
            'trend': trend
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/models/compare', methods=['GET'])
def compare_models():
    """Compare performance across models (Spark and Hadoop)"""
    if db is None:
        return jsonify({'error': 'Database not available'}), 500
    
    hours = int(request.args.get('hours', 24))
    time_threshold = datetime.now() - timedelta(hours=hours)
    
    # Pipeline for Spark batch_metrics
    spark_pipeline = [
        {'$match': {'timestamp': {'$gte': time_threshold}}},
        {'$group': {
            '_id': '$model',
            'avg_accuracy': {'$avg': '$accuracy'},
            'avg_f1_score': {'$avg': '$f1_score'},
            'avg_precision': {'$avg': '$precision'},
            'avg_recall': {'$avg': '$recall'},
            'total_batches': {'$sum': 1},
            'total_tweets': {'$sum': '$total_tweets'},
            'avg_processing_time': {'$avg': '$processing_time_ms'}
        }},
        {'$project': {
            'model': '$_id',
            'source': {'$literal': 'spark'},
            'avg_accuracy': 1,
            'avg_f1_score': 1,
            'avg_precision': 1,
            'avg_recall': 1,
            'total_batches': 1,
            'total_tweets': 1,
            'avg_processing_time_seconds': {'$divide': ['$avg_processing_time', 1000]}
        }}
    ]
    
    # Pipeline for Hadoop hadoop_prediction_batches
    hadoop_pipeline = [
        {'$match': {'timestamp': {'$gte': time_threshold}}},
        {'$group': {
            '_id': '$model',
            'avg_accuracy': {'$avg': '$accuracy'},
            'avg_f1_score': {'$avg': '$f1_score'},
            'avg_precision': {'$avg': '$precision'},
            'avg_recall': {'$avg': '$recall'},
            'total_batches': {'$sum': 1},
            'total_tweets': {'$sum': '$tweets_processed'},
            'avg_processing_time': {'$avg': '$execution_time_seconds'}
        }},
        {'$project': {
            'model': '$_id',
            'source': {'$literal': 'hadoop'},
            'avg_accuracy': 1,
            'avg_f1_score': 1,
            'avg_precision': 1,
            'avg_recall': 1,
            'total_batches': 1,
            'total_tweets': 1,
            'avg_processing_time_seconds': '$avg_processing_time'
        }}
    ]
    
    try:
        spark_comparison = list(db.batch_metrics.aggregate(spark_pipeline))
        hadoop_comparison = list(db.hadoop_prediction_batches.aggregate(hadoop_pipeline))
        
        # Combine results
        comparison = spark_comparison + hadoop_comparison
        
        return jsonify({
            'time_range_hours': hours,
            'models': comparison
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/search/tweets', methods=['GET'])
def search_tweets():
    """Search tweets by text"""
    if db is None:
        return jsonify({'error': 'Database not available'}), 500
    
    query_text = request.args.get('q', '')
    model = request.args.get('model', None)
    limit = int(request.args.get('limit', 50))
    
    if not query_text:
        return jsonify({'error': 'Query parameter "q" is required'}), 400
    
    search_query = {'text': {'$regex': query_text, '$options': 'i'}}
    if model:
        search_query['model'] = model
    
    try:
        results = list(db.predictions.find(
            search_query,
            {'_id': 0}
        ).limit(limit))
        
        return jsonify({
            'query': query_text,
            'count': len(results),
            'results': results
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/dashboard')
def dashboard():
    """Serve dashboard HTML"""
    return send_file('templates/dashboard.html')


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Starting Sentiment Analysis API")
    print("=" * 60)
    print(f"MongoDB URI: {MONGO_URI}")
    print("Endpoints:")
    print("  - Health Check: http://localhost:5000/api/health")
    print("  - Dashboard: http://localhost:5000/dashboard")
    print("  - API Root: http://localhost:5000/")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)
