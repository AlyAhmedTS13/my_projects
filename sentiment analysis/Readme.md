# 🎧 Reddit Pop Sentiment Analysis  
**End-to-End NLP Pipeline for Classifying Sentiment in Pop Music Discussions**

---

## 📌 Project Overview

This project explores how Reddit users express sentiment around pop music and pop stars. We built a full pipeline—from data collection to model deployment—to classify short-form Reddit texts into three sentiments: **positive**, **neutral**, or **negative**.

Our goal was to create a **realistic, scalable baseline** for sentiment analysis in noisy, user-generated content. We used a **BERT model for pseudo-labeling** and trained a **Linear SVM classifier** using TF-IDF features. Despite class imbalance and informal language, our model achieved **92.9% train accuracy** and **76.1% test accuracy**, which is **acceptable and expected in text classification**, especially under real-world conditions.

---

## 🧠 Motivation & Decisions

- **Why Reddit?**  
  Reddit offers rich, organic discussions—ideal for capturing real-world sentiment around pop culture.

- **Why pseudo-labeling?**  
  Manual labeling 32k+ rows wasn’t feasible. We used a **pretrained BERT model** to assign sentiment labels automatically, giving us a scalable and reasonably accurate training set.

- **Why BERT over VADER?**  
  We initially tested **VADER**, but found it too simplistic for our domain:
  - It leaned heavily toward **positive sentiment**, even for neutral or factual posts.
  - It misclassified nuanced or mixed statements.
  - It lacked contextual understanding, especially for negations like “never disappoints.”
  
  In contrast, **BERT (CardiffNLP Twitter RoBERTa)** was:
  - Trained on social media text.
  - Better at handling short-form, informal language.
  - More realistic in assigning **neutral** to factual statements.
  
  **Conclusion:** BERT produced more reliable pseudo-labels for training a robust classifier.

- **Why Linear SVM?**  
  We chose **LinearSVC** because:
  - It’s optimized for **high-dimensional, sparse text data**.
  - It’s fast, interpretable, and ideal for TF-IDF features.
  - It performs well on short-form social media text without needing deep architectures.

---

## 🛠️ Pipeline Summary

### 1. **Data Collection**
- **Tool:** Reddit API via `praw`
- **Subreddits:** Music-focused communities like `popheads`, `Music`, `TaylorSwift`, `Billboard`, etc.
- **Keywords:** Pop artists (Taylor Swift, Olivia Rodrigo, Ariana Grande, etc.), albums, songs, awards, fandom terms.
- **Volume:** ~124,000 posts collected
- **Fields:** `title`, `selftext`, `author`, `subreddit`, `score`, `comments`, `timestamp`, `keyword`, `source`

## 3. **Data Cleaning**

### Noise Reduction (Critical Step)
- **On-topic pruning:** Applied per‑subreddit allowed keyword lists to drop irrelevant combinations.  
- **Examples:** Stricter allowlists for `NoStupidQuestions`, `teenagers`, `Fauxmoi`, etc.  
- **Why:** Subreddit names alone don’t guarantee pop relevance; keyword context ensures we keep only **pop-central posts**.

### Combine Fields to Text
- **Step:**  
  ```python
  text = title + " " + selftext.fillna("")
  text = text.strip()
- **Why:** Reddit posts often place sentiment in titles; combining captures both headline sentiment and body detail.

### Short-form Filter (≤ 280 characters)

- **Why:**  
  - Matches social media brevity where sentiment is most explicit.  
  - Enables faster inference and reduces truncation issues for transformer models.  
  - Reduces variance from long, multi-topic paragraphs that often muddy sentiment signals.

- **Result:**  
  Reduced dataset size from ~124k rows to ~32k focused, short-form rows.

- **Outcome:**  
  A **pop-focused, short-form dataset** that realistically reflects **social media sentiment patterns** and **Reddit-specific discourse structure**, making it more suitable for sentiment classification tasks.


### 3. **Sentiment Labeling (Pseudo-labels)**
- Used **BERT (Twitter RoBERTa)** for inference
- Final labels: `positive`, `neutral`, `negative`
- Distribution:
  - **Neutral** dominant (factual/news posts)
  - **Positive** next (praise, excitement)
  - **Negative** least (criticism, disappointment)

### 4. **Text Preprocessing**

- **Stopwords with negations kept**  
  **Why:** Negations like “not,” “don’t,” “never,” and “won’t” are critical in sentiment analysis. Removing them can flip the meaning of a sentence and lead to incorrect classification. For example, “I don’t like this” without “don’t” becomes “I like this,” which is the opposite sentiment.

- **Normalization**  
  **Steps:**  
  - Lowercasing all text  
  - Removing punctuation and emojis using regular expressions  
  - Trimming extra whitespace  
  **Trade-off:** While removing emojis and punctuation simplifies the feature space, it also removes expressive cues like “!” or “🥹” that often carry emotional weight. For this baseline model, the trade-off was acceptable to maintain simplicity and speed.

- **Lemmatization**  
  **Tool:** `WordNetLemmatizer` from NLTK  
  **Why:** Lemmatization reduces words to their base form (e.g., “loves” → “love”), which helps the TF-IDF vectorizer generalize across similar word variants and reduces feature sparsity.

- **Vectorization**  
  **Approach:** TF-IDF (`TfidfVectorizer`) applied to the cleaned text  
  **Why:** TF-IDF is ideal for sparse, high-dimensional text data. It captures term importance relative to the corpus without requiring deep computation. It’s well-suited for linear models like SVM.

- **Label Mapping**  
  **Classes:**  
  - `negative` → 0  
  - `neutral` → 1  
  - `positive` → 2  
  **Why:** Numerical encoding is required for scikit-learn classifiers and ensures consistent metric computation across training and evaluation.


### 5. **Model Training**
- **Model:** `LinearSVC(class_weight='balanced')`
- **Split:** 80/20 train-test with `stratify=y`
- **Vectorizer:** `TfidfVectorizer()`

#### Why `class_weight='balanced'`?
- Our dataset was **imbalanced**, with neutral dominating.
- Instead of oversampling or undersampling—which can cause overfitting or loss of valuable data—we used class weights to give minority classes more influence during training.
- **Oversampling** duplicates data and risks overfitting, especially in text.
- **Data augmentation** (e.g., paraphrasing) is risky in NLP because it can **change sentiment**, leading to incorrect labels.

#### Why no oversampling or augmentation?
- **Oversampling:** Can cause the model to memorize duplicated text, reducing generalization.
- **Augmentation:** In sentiment tasks, changing wording can flip sentiment (e.g., “I love this” → “I don’t mind this”), making it unsafe without human review.

---

## 📊 Model Performance

### Train Metrics
- **Accuracy:** 92.97%
- **Precision (weighted):** 93.79%
- **Recall (weighted):** 92.97%
- **F1 Score (weighted):** 93.32%

### Test Metrics
- **Accuracy:** 76.15%
- **Precision (weighted):** 76.12%
- **Recall (weighted):** 76.15%
- **F1 Score (weighted):** 76.13%

### Why the gap is acceptable
- In **text classification**, especially with noisy, user-generated data, it’s common to see a gap between train and test accuracy.
- Our model generalizes well despite pseudo-label noise and class imbalance.
- The ~16% gap is **not severe overfitting**, but reflects the complexity of real-world sentiment and the limitations of classical models.

### Per-class metrics
| Label     | Precision | Recall | F1-score |
|-----------|-----------|--------|----------|
| Negative  | 0.57      | 0.59   | 0.58     |
| Neutral   | 0.82      | 0.88   | 0.85     |
| Positive  | 0.70      | 0.66   | 0.68     |

### Grid Search
- Tried hyperparameter tuning
- **Improved accuracy by ~1%**
- Indicates we’re near the ceiling for this setup

---

## 🔍 Manual Testing & Model Strengths

We tested the model on 40+ manually written examples covering clear, mixed, and nuanced sentiment.

### Strengths
- **Clear positive/negative sentiment:**  
  - “I love the new Taylor Swift album” → positive  
  - “I hate pop music” → negative
- **Factual/news statements:**  
  - “Olivia Rodrigo releases new single” → neutral  
  - “Taylor Swift wins Grammy” → neutral

### Weaknesses
- **Negation handling:**  
  - “Taylor Swift never disappoints me” → misclassified as negative
- **Mixed sentiment:**  
  - “I love the melodies but the lyrics are weak” → hard to classify
- **Slang, emojis, punctuation:**  
  - Lost intensity without “!” or emojis
- **Sarcasm and subtlety:**  
  - Classical models can’t detect tone or irony

---

## 🧠 What We Learned

- **Neutral dominates pop discourse:**  
  Reddit’s music talk is often informative. Our model reflects that reality well.

- **Pseudo-label quality sets the ceiling:**  
  BERT labels were far more realistic than VADER, especially for neutral and mixed posts.

- **Short-form filtering improves performance:**  
  Trimming to ≤280 characters helped the model focus on sentiment-bearing text.

- **Linear SVM is a strong baseline for text:**  
  Fast, interpretable, and effective—especially with TF-IDF and class weights.

- **Overfitting isn’t fatal in text classification:**  
  Some gap is expected due to label noise and domain complexity. Our model still generalizes well.

- **Avoiding oversampling and augmentation was the right call:**  
  Both techniques carry risks in NLP—either overfitting or sentiment distortion.

---

## 🚀 Future Directions

- Fine-tune a transformer model on our labeled data
- Add emoji/slang handling
- Improve negation detection
- Use human-in-the-loop label refinement
- Explore ensemble models or contextual embeddings

---

## 🏁 Final Thoughts

We built a grounded, end-to-end sentiment pipeline tailored to Reddit’s pop music discourse. By choosing BERT pseudo-labels over VADER, curating short-form texts, and training a LinearSVC baseline, we struck a practical balance: realistic labels, fast modeling, and transparent evaluation.

The model is **highly effective for clear sentiment and factual content**, and it surfaces exactly where deeper language understanding is needed next. It’s a strong baseline—ready to scale with richer features and deeper models.
