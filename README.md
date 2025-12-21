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

| Implementation  | Complexity | Speed  | Accuracy | Best For           |
| --------------- | ---------- | ------ | -------- | ------------------ |
| Hadoop Basic NB | Low        | Slower | ~70-75%  | Learning MapReduce |
| Spark NB/SVM    | Medium     | Fast   | ~75-80%  | Production use     |

---

## 🛠️ **Technologies Used**

- **Big Data**: Hadoop MapReduce, Apache Spark
- **Languages**: Java, Scala, Python
- **Storage**: HDFS, MongoDB
- **Visualization**: webUI

---

## 📊 **Detailed Results Comparison**

### **Model Performance Matrix**

| Model             | Dataset Size | Training Time | Accuracy | Precision (Pos) | Recall (Pos) | F1-Score |
| ----------------- | ------------ | ------------- | -------- | --------------- | ------------ | -------- |
| Hadoop Basic NB   | 1.6M         | ~15-20 min    | ~72%     | N/A             | N/A          | N/A      |
| Spark NB (MLlib)  | 1.6M         | ~5-8 min      | ~76%     | ~0.77           | ~0.76        | ~0.76    |
| Spark SVM (MLlib) | 1.6M         | ~8-12 min     | ~78%     | ~0.79           | ~0.78        | ~0.78    |

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
