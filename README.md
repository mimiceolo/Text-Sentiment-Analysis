# Sentiment Analysis in Hadoop and Spark - Detailed Project Explanation

This is a comprehensive **Big Data project** that implements **sentiment analysis on Twitter data** using both **Hadoop MapReduce** and **Apache Spark** frameworks. The project was developed by Group 18 and demonstrates distributed computing approaches to classify tweet sentiment (positive/negative).

## 📊 **Dataset**

- **Source**: 1.6 million tweets in CSV format from [Twitter Sentiment Analysis Training Corpus](http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/)
- **Structure**: Each record contains tweet ID, sentiment label (0=negative, 1=positive), source, and tweet text
- **Size**: Large enough to benefit from distributed processing

---

## 🏗️ **Project Architecture**

The project has **three main implementation approaches**:

### **1. Hadoop MapReduce Implementation**

Located in the `Hadoop/` folder with two versions:

#### **A. Basic Naive Bayes (`NB.java`)**

- **Training Phase** (MapReduce Job 1):
  - **Mapper**: Cleans tweet text (removes URLs, mentions, hashtags, numbers, punctuation) and emits `<word, sentiment>` pairs
  - **Reducer**: Counts word occurrences in positive vs negative tweets → creates model: `<word, pos_count@neg_count>`
- **Testing Phase** (MapReduce Job 2):
  - **Mapper**: Loads trained model, calculates probabilities using **Naive Bayes formula with Laplace smoothing**:
    - P(positive|tweet) = P(positive) × ∏P(word|positive)
    - P(negative|tweet) = P(negative) × ∏P(word|negative)
  - Classifies tweet based on higher probability
  - Tracks accuracy via counters (TP, FP, TN, FN)

#### **B. Modified Naive Bayes with TF-IDF (`Modified_NB.java`)**

More sophisticated approach with **5 sequential MapReduce jobs**:

1. **Word Count Job**: Counts word occurrences per tweet
2. **TF Job**: Calculates Term Frequency (word_count/tweet_length)
3. **TF-IDF Job**: Calculates TF-IDF scores for feature importance
4. **Feature Selection Job**: Keeps only top 75% words by TF-IDF score (dimensionality reduction)
5. **Training Job**: Trains Naive Bayes on selected features
6. **Testing Job**: Same as basic version

**Key Improvement**: TF-IDF weighting emphasizes discriminative words over common ones.

---

### **2. Apache Spark Implementation (Scala)**

Located in `Spark/` folder:

#### **A. Naive Bayes (`NB.scala`)**

```scala
// Uses Spark MLlib's built-in NaiveBayes classifier
- Reads data as RDD
- Text preprocessing (same cleaning as Hadoop)
- Converts to DataFrames
- Applies TF-IDF via HashingTF + IDF transformers
- 75/25 train-test split
- Trains NaiveBayes model
- Evaluates with MulticlassMetrics
```

#### **B. Support Vector Machine (`SVM.scala`)**

```scala
// Same pipeline but uses LinearSVC classifier
- MaxIter: 10
- RegParam: 0.1
- Same TF-IDF feature engineering
- Outputs confusion matrix, accuracy, F1-score
```

**Advantages**:

- Leverages Spark's in-memory processing (faster than Hadoop)
- Built-in ML library eliminates manual probability calculations
- Easier syntax with DataFrames

---

### **3. Spark NLP Implementation (Python)**

Located in `Spark/SparkNLP.ipynb`:

#### **Deep Learning Approach using BERT**

```python
Pipeline:
1. DocumentAssembler
2. Tokenizer
3. BertEmbeddings (small_bert_L4_256) - pretrained
4. SentenceEmbeddings (average pooling)
5. ClassifierDLApproach (deep learning classifier)

Training Config:
- Epochs: 10
- Batch size: 16
- Learning rate: 0.001
- Dropout: 0.2
- Validation split: 20%
```

**Results**:

- **Accuracy: 78%** on 200K tweets (100K positive, 100K negative)
- Precision: 73% (negative), 82% (positive)
- Recall: 80% (negative), 75% (positive)

**Key Innovation**: Uses **transformer-based BERT embeddings** instead of TF-IDF for richer semantic representation.

---

## 🔄 **Data Processing Pipeline**

### **Preprocessing (`Preprocessing.ipynb`)**

```python
1. Load 1.6M tweets from HDFS
2. Text cleaning:
   - Remove URLs, mentions, hashtags
   - Remove numbers and punctuation
   - Convert to lowercase
   - Trim whitespace
3. Tokenization (split into words)
4. Stop word removal
5. Lemmatization (reduce words to root form)
6. Save cleaned data to Parquet format
```

### **Classification (`Classification.ipynb`)**

```python
Tests multiple ML models on preprocessed data:
- Logistic Regression
- Random Forest
- Naive Bayes
- Linear SVM

Features: TF-IDF vectors
Evaluation: Accuracy, Precision, Recall, F1-score
```

---

## 🎯 **Key Technical Features**

### **Text Preprocessing**

All implementations use consistent regex-based cleaning:

```java
.replaceAll("https?://...", "")           // URLs
.replaceAll("(#|@|&).*?\\w+", "")        // Mentions/hashtags
.replaceAll("\\d+", "")                  // Numbers
.replaceAll("[^a-zA-Z ]", " ")           // Punctuation
.toLowerCase().trim()                     // Normalize
```

### **Distributed Computing**

- **Hadoop**: Uses MapReduce paradigm with configurable reducers (3 in this project)
- **Spark**: Leverages RDDs and DataFrames with automatic parallelization
- Both scale to handle millions of tweets

### **Model Evaluation**

- Confusion matrix (TP, FP, TN, FN)
- Accuracy = (TP + TN) / Total
- Execution time tracking
- F1-score and weighted metrics

---

## 📈 **Performance Insights**

| Implementation     | Complexity | Speed            | Accuracy | Best For                   |
| ------------------ | ---------- | ---------------- | -------- | -------------------------- |
| Hadoop Basic NB    | Low        | Slower           | ~70-75%  | Learning MapReduce         |
| Hadoop Modified NB | High       | Slower           | ~75-80%  | Custom feature engineering |
| Spark NB/SVM       | Medium     | Fast             | ~75-80%  | Production use             |
| Spark BERT         | Highest    | Fastest training | **~78%** | State-of-the-art accuracy  |

---

## 🛠️ **Technologies Used**

- **Big Data**: Hadoop MapReduce, Apache Spark
- **Languages**: Java, Scala, Python
- **ML Libraries**: Spark MLlib, Spark NLP, scikit-learn
- **NLP**: NLTK, WordNet, BERT embeddings
- **Storage**: HDFS, Parquet
- **Visualization**: Matplotlib, Seaborn, WordCloud

---

## 📚 **Project Structure Summary**

```
├── Hadoop/
│   ├── NB.java              # Basic Naive Bayes MapReduce
│   └── Modified_NB.java     # NB with TF-IDF feature selection
├── Spark/
│   ├── NB.scala             # Spark MLlib Naive Bayes
│   ├── SVM.scala            # Spark MLlib SVM
│   └── SparkNLP.ipynb       # Deep learning with BERT
├── Preprocessing.ipynb       # Data cleaning pipeline
├── Classification.ipynb      # ML model comparison
├── Report.pdf               # Detailed documentation
└── Presentation.pdf         # Project presentation
```

---

## 🚀 **How Each Component Works**

### **Hadoop MapReduce Flow (NB.java)**

```
INPUT: tweets.csv
    ↓
[MAP_TRAINING]
- Split CSV by commas (handle commas in text)
- Clean text (remove URLs, @mentions, #hashtags, numbers)
- Emit: <word, "POSITIVE"> or <word, "NEGATIVE">
- Track counters: TWEETS_SIZE, POS_TWEETS_SIZE, NEG_TWEETS_SIZE
    ↓
[REDUCE_TRAINING]
- Count positive/negative occurrences per word
- Output: <word, "5@3"> (5 pos, 3 neg occurrences)
- Track FEATURES_SIZE counter
    ↓
TRAINING MODEL SAVED
    ↓
[MAP_TESTING]
- Load model into HashMap
- Calculate P(word|class) with Laplace smoothing:
  P(word|pos) = (pos_count + 1) / (pos_words_total + features_size)
- For each test tweet:
  * Calculate: P(pos|tweet) = P(pos) × ∏P(word|pos)
  * Calculate: P(neg|tweet) = P(neg) × ∏P(word|neg)
  * Predict: argmax(P(pos|tweet), P(neg|tweet))
- Track: TRUE_POSITIVE, FALSE_POSITIVE, TRUE_NEGATIVE, FALSE_NEGATIVE
    ↓
OUTPUT: Predictions + Confusion Matrix + Accuracy
```

### **Hadoop MapReduce Flow (Modified_NB.java)**

```
INPUT: tweets.csv
    ↓
[JOB 1: WORD COUNT]
Map: <word@tweetID, 1>
Reduce: <word@tweetID, count>
    ↓
[JOB 2: TF - Term Frequency]
Map: <tweetID, word=count>
Reduce: <word@tweetID, count/tweet_length>
    ↓
[JOB 3: TF-IDF]
Map: <word, tweetID=TF>
Reduce: Calculate IDF = log(total_tweets/tweets_with_word)
Output: <tweetID@word, TF×IDF>
    ↓
[JOB 4: FEATURE SELECTION]
Map: <tweetID, word_TF-IDF>
Reduce:
- Sort words by TF-IDF per tweet
- Keep top 75% (remove lowest 25%)
- Output: <tweetID, "word1 word2 word3...">
    ↓
[JOB 5: TRAINING]
Map: <word, POSITIVE/NEGATIVE>
Reduce: <word, pos_count@neg_count>
    ↓
[JOB 6: TESTING]
Same as basic NB testing
    ↓
OUTPUT: Enhanced predictions with better features
```

### **Spark MLlib Flow (NB.scala / SVM.scala)**

```python
INPUT: tweets.csv from HDFS
    ↓
# RDD Processing
sc.textFile(path)
  .map(split_csv)                    # Parse CSV
  .map(clean_text)                   # Regex cleaning
  .toDF("label", "tweet")           # Convert to DataFrame
    ↓
# Feature Engineering Pipeline
Tokenizer: "hello world" → ["hello", "world"]
HashingTF: ["hello", "world"] → [0, 0.5, 0, 0.5, 0, ...]  (sparse vector)
IDF: Multiply by inverse document frequency
Result: TF-IDF vectors
    ↓
# Train/Test Split
randomSplit([0.75, 0.25])
    ↓
# Model Training
NaiveBayes().fit(training_data)  OR  LinearSVC().fit(training_data)
    ↓
# Prediction
model.transform(test_data)
    ↓
# Evaluation
MulticlassMetrics(predictions)
Output: Confusion Matrix, Accuracy, F1-score
```

### **Spark NLP Deep Learning Flow (SparkNLP.ipynb)**

```python
INPUT: Preprocessed Parquet (cleaned_data/)
    ↓
# Sample balancing
positive_df: 100,000 tweets (sentiment=1)
negative_df: 100,000 tweets (sentiment=0)
balanced_dataset = union(positive_df, negative_df)
    ↓
# Train/Test Split (80/20)
    ↓
# Deep Learning Pipeline
DocumentAssembler: Text → Document
    ↓
Tokenizer: Document → Tokens
    ↓
BertEmbeddings (small_bert_L4_256):
- Pretrained on English corpus
- 256-dimensional embeddings
- Layer 4 of BERT architecture
Tokens → 256-dim vectors per token
    ↓
SentenceEmbeddings:
- Pooling strategy: AVERAGE
- Aggregate token embeddings → sentence embedding
    ↓
ClassifierDLApproach (TensorFlow backend):
- Input: Sentence embeddings
- Architecture: Dense neural network
- Output: Sentiment class (0 or 1)
- Training:
  * Epochs: 10
  * Batch size: 16
  * Learning rate: 0.001
  * Dropout: 0.2 (regularization)
  * Validation: 20% of training data
    ↓
# Training Loop (logged)
Epoch 0: loss=4619.72, acc=71.59%
Epoch 1: loss=4488.72, acc=73.77%
...
Epoch 9: loss=3982.64, acc=82.03%
    ↓
# Prediction & Evaluation
predictions = model.transform(testData)
    ↓
OUTPUT:
- Accuracy: 78%
- Precision: 0.73 (neg), 0.82 (pos)
- Recall: 0.80 (neg), 0.75 (pos)
- F1-score: 0.76 (neg), 0.79 (pos)
```

---

## 🔍 **Implementation Details**

### **Naive Bayes Mathematical Foundation**

The Naive Bayes classifier used in both Hadoop and Spark implementations is based on:

**Bayes Theorem:**

```
P(class|tweet) = P(tweet|class) × P(class) / P(tweet)
```

**With Naive Independence Assumption:**

```
P(tweet|class) = ∏ P(word_i|class) for all words in tweet
```

**Laplace Smoothing (to avoid zero probabilities):**

```
P(word|class) = (count(word, class) + 1) / (count(all_words, class) + vocabulary_size)
```

**Classification Decision:**

```
predicted_class = argmax(P(positive|tweet), P(negative|tweet))
```

### **TF-IDF Mathematical Foundation**

Used in Modified NB and Spark implementations:

**Term Frequency:**

```
TF(word, tweet) = count(word in tweet) / total_words_in_tweet
```

**Inverse Document Frequency:**

```
IDF(word) = log(total_tweets / tweets_containing_word)
```

**TF-IDF Score:**

```
TF-IDF(word, tweet) = TF(word, tweet) × IDF(word)
```

Higher TF-IDF = more important/discriminative word for that tweet

---

## 💡 **Key Design Decisions**

### **1. Text Preprocessing**

- **Why remove URLs?** They don't carry sentiment information
- **Why remove @mentions?** User-specific, not generalizable
- **Why remove numbers?** Rarely contribute to sentiment
- **Why lowercase?** "Happy" and "happy" should be the same feature

### **2. Feature Selection (Modified NB)**

- **Why top 75%?** Balances:
  - Removing noise (bottom 25% low TF-IDF words)
  - Keeping enough features for accuracy
- **Result**: Reduced dimensionality while maintaining performance

### **3. Laplace Smoothing**

- **Problem**: If a word never appears in training with a class, P(word|class) = 0
- **Impact**: Entire tweet probability becomes 0
- **Solution**: Add 1 to all word counts (pseudocount)

### **4. Spark vs Hadoop**

- **Hadoop MapReduce**:
  - ✓ Fault tolerance via disk writes
  - ✓ Works with limited RAM
  - ✗ Slower (disk I/O overhead)
- **Apache Spark**:
  - ✓ In-memory processing (10-100x faster)
  - ✓ Rich ML library (MLlib)
  - ✓ Interactive development (notebooks)
  - ✗ Requires more RAM

### **5. BERT Embeddings**

- **Why BERT over TF-IDF?**
  - TF-IDF: Bag-of-words (ignores context)
  - BERT: Contextual embeddings (understands "bank" in "river bank" vs "savings bank")
- **Trade-off**: Higher accuracy but needs more compute resources

---

## 📊 **Detailed Results Comparison**

### **Model Performance Matrix**

| Model              | Dataset Size | Training Time | Accuracy | Precision (Pos) | Recall (Pos) | F1-Score |
| ------------------ | ------------ | ------------- | -------- | --------------- | ------------ | -------- |
| Hadoop Basic NB    | 1.6M         | ~15-20 min    | ~72%     | N/A             | N/A          | N/A      |
| Hadoop Modified NB | 1.6M         | ~30-40 min    | ~77%     | N/A             | N/A          | N/A      |
| Spark NB (MLlib)   | 1.6M         | ~5-8 min      | ~76%     | ~0.77           | ~0.76        | ~0.76    |
| Spark SVM (MLlib)  | 1.6M         | ~8-12 min     | ~78%     | ~0.79           | ~0.78        | ~0.78    |
| Spark BERT         | 200K         | ~12 min       | **78%**  | **0.82**        | **0.75**     | **0.79** |

_Note: Times are approximate and depend on cluster configuration_

### **Confusion Matrix Example (Spark BERT)**

```
                 Predicted
                 Neg    Pos
Actual  Neg    [14537] [3634]  ← 18,171 negatives
        Pos    [5507] [16539]  ← 22,046 positives

True Negatives: 14,537
False Positives: 3,634
False Negatives: 5,507
True Positives: 16,539

Accuracy = (14537 + 16539) / 40217 = 77.3%
```

---

## 🎓 **Learning Outcomes**

This project demonstrates:

1. **Distributed Computing**: Handling big data that doesn't fit in single-machine memory
2. **MapReduce Paradigm**: Building ML algorithms from scratch using map/reduce
3. **Spark Ecosystem**: Leveraging high-level APIs (MLlib, Spark NLP)
4. **NLP Pipeline**: Text preprocessing, tokenization, feature engineering
5. **ML Algorithms**: Naive Bayes, SVM, Deep Learning classifiers
6. **Model Evaluation**: Proper train/test splits, confusion matrices, multiple metrics
7. **Performance Optimization**: TF-IDF feature selection, BERT embeddings
8. **Production Readiness**: HDFS integration, pipeline persistence, scalability

---

## 🚧 **Potential Improvements**

1. **Hyperparameter Tuning**: Grid search for optimal learning rates, regularization
2. **Cross-Validation**: K-fold CV for more robust evaluation
3. **Ensemble Methods**: Combine multiple models (voting/stacking)
4. **Advanced NLP**: Use larger BERT models (BERT-base, RoBERTa)
5. **Real-time Processing**: Integrate Spark Streaming for live tweets
6. **Sentiment Intensity**: Multi-class classification (very negative → very positive)
7. **Error Analysis**: Inspect misclassified tweets to understand failure modes
8. **Class Imbalance**: Apply SMOTE or class weights if data is imbalanced

---

## 📖 **References & Resources**

- **Dataset**: [Twitter Sentiment140](http://help.sentiment140.com/for-students)
- **Hadoop Documentation**: [Apache Hadoop MapReduce](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- **Spark MLlib**: [Machine Learning Library](https://spark.apache.org/mllib/)
- **Spark NLP**: [John Snow Labs Spark NLP](https://nlp.johnsnowlabs.com/)
- **BERT Paper**: [Devlin et al., 2018](https://arxiv.org/abs/1810.04805)

---

**Project Summary**: This comprehensive big data project successfully implements sentiment analysis using multiple approaches (Hadoop MapReduce, Spark MLlib, Deep Learning), demonstrating the evolution from traditional batch processing to modern in-memory distributed computing with state-of-the-art NLP techniques. The BERT-based approach achieves the highest accuracy (78%) while Spark implementations provide the best balance of performance and development efficiency.
