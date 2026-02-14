# 💳 Fraud Detection System — PaySim Transactions

## 📌 Project Overview
Financial fraud is a major risk in digital payment systems.  
This project builds an end-to-end fraud detection pipeline using the PaySim simulated mobile money dataset, combining:

- SQL-based data cleaning and feature engineering  
- Exploratory Data Analysis (EDA) in SQL  
- Machine Learning anomaly detection in Python  
- Business rule simulation and risk scoring  
- Interactive Power BI dashboard for insights and model monitoring  

The final system mimics how real-world fraud detection systems score transactions, set operational thresholds, and separate transactions into **Allow / Review / Block** decisions.

---

## 🧠 Problem Statement
Given millions of mobile money transactions, detect fraudulent ones efficiently while:

- Catching as much fraud as possible (**high recall**)  
- Keeping false alarms manageable (**reasonable precision**)  
- Supporting realistic operational review capacity  

---

## 📂 Dataset
**PaySim** — A simulated mobile money transaction dataset containing:

- Transaction types (CASH_OUT, TRANSFER, etc.)  
- Sender and receiver balances before/after transaction  
- Amounts  
- Time steps (hours)  
- Ground-truth fraud label (`isFraud`)  

Fraud occurs only in:

- **TRANSFER**  
- **CASH_OUT**  

This fact guided later feature engineering and model focus.

---

## 🧹 SQL Data Cleaning
Performed in **MySQL**:

### ✔ Data Validation Checks
- Removed impossible values:  
  - Negative or zero transaction amounts  
  - Negative balances  
- Verified no null values in key columns  

**Result:**  
- Clean dataset with valid numeric ranges  
- No missing categorical or ID fields  

### ✔ Indexing
Indexes were added to frequently queried fields:
- `nameOrig` (sender)  
- `nameDest` (receiver)  
- `type`  
- `hour_of_day`  

This significantly improved EDA query performance.

---

## ⚙️ SQL Feature Engineering
To expose fraud signals, several **risk flag features** were created — inspired by real fraud-engineering practices.

| Feature                   | Description                                | Business Motivation                         |
|----------------------------|--------------------------------------------|---------------------------------------------|
| balance_mismatch_flag      | Sender balance doesn’t update correctly    | Indicates manipulated ledger behavior        |
| overdraft_flag             | Transaction amount exceeds sender balance  | Aggressive or suspicious spending            |
| drained_account_flag       | Account emptied after transaction          | Common fraud exit pattern                    |
| dest_balance_mismatch_flag | Receiver balance inconsistency             | Possible tampering                           |
| high_amount_ratio_flag     | Amount ≥ 90% of sender balance             | Risky high-impact transaction                |
| night_transaction_flag     | Transaction during late night hours        | Fraud often happens when users inactive      |
| hour_of_day                | Derived from step % 24                     | Temporal fraud pattern analysis              |

All flags were created only for **TRANSFER** and **CASH_OUT** transactions — where fraud exists.  
These engineered features were later fed into the ML model.

---

## 🔍 SQL Exploratory Data Analysis (EDA)
Key investigations performed:

- **Fraud Statistics**  
  - Overall fraud rate  
  - Fraud counts vs non-fraud  

- **Transaction Distribution**  
  - Volume per transaction type  
  - Fraud rate per type  

- **Balance Risk Signals**  
  - Fraud rate when balance mismatch flag triggered  
  - Fraud rate when drained account flag triggered  

- **Behavioral Patterns**  
  - Fraud rate by hour of day  
  - Multi-transaction senders  
  - Destinations receiving from many unique senders  

- **Risk Signal Aggregation**  
  - `sum_of_flags_triggered = sum(all six flags)`  
  - Fraud rate analyzed per number of triggered flags → higher flags = higher fraud likelihood  

This validated that engineered features carried real predictive signal.

---

## 🤖 Machine Learning — Python Modeling
### 🎯 Objective
Train an anomaly detection model to assign a fraud risk score per transaction.

### 🧪 Approach
- Selected only **TRANSFER** and **CASH_OUT**  
- Used engineered numeric + flag features  
- Standard scaling  
- Trained anomaly detection model  
- Produced continuous `anomaly_score`  

### 📈 Model Performance
**ROC-AUC ≈ 0.964** → Excellent ranking ability between fraud and normal transactions.

#### Threshold Evaluation

| Top % flagged | Threshold | Precision | Recall |
|---------------|-----------|-----------|--------|
| 0.05%         | 0.8787    | 0.49      | 0.083  |
| 0.10%         | 0.8564    | 0.42      | 0.142  |
| 0.30%         | 0.8112    | 0.22      | 0.219  |
| 0.50%         | 0.7805    | 0.17      | 0.289  |
| 1.00%         | 0.7305    | 0.12      | 0.418  |
| 2.00%         | 0.6772    | 0.084     | 0.567  |

**Interpretation:**  
- Higher threshold → higher precision → fewer alerts → lower recall  
- Lower threshold → more fraud caught → more false positives  

This is the classic fraud-detection tradeoff.

---

## 🧩 Business Decision Layer
Final transaction decisions:

| Decision | Meaning                                   |
|----------|-------------------------------------------|
| Block    | High-risk transactions automatically rejected |
| Review   | Medium-risk transactions sent to manual analysts |
| Allow    | Low-risk transactions pass normally       |

### 🎯 Final Threshold Strategy
Based on operational realism:

| Segment | Threshold   | % of Transactions | Precision | Recall | Purpose                          |
|---------|-------------|-------------------|-----------|--------|----------------------------------|
| Block   | ≥ 0.856     | Top 0.1%          | ~42%      | ~14%   | High-confidence fraud auto-block |
| Review  | 0.780–0.856 | Next 0.4%         | ~17–22%   | ~22–29%| Dashboard review queue           |
| Allow   | < 0.780     | Remaining 99.5%   | very low  | —      | Normal processing                |

This mirrors how real fraud engines operate.

---

## ⚖️ Why Not Force 100% Recall?
Catching 100% of fraud means flagging almost every transaction — impossible operationally.  
Real systems balance:

- Analyst review capacity  
- Customer friction  
- Fraud loss tolerance  

Thresholds are chosen by **business cost optimization**, not only ML metrics.

---

## 🚧 Challenge Faced — Rule Weights vs ML Score
Initially, rule-based weighted risk scores were combined with ML score.  
**Result:** ML-only score outperformed weighted combination.

**Why?**
- Rule flags were already fed into ML  
- Adding manual weights re-introduced noise  
- No new external signals were added  

**Lesson Learned:**  
Rules add value only when they use external signals not present in ML features.  
Since our rules were derived from the same columns, ML alone captured their signal better.

---

## 📊 Power BI Dashboard
An interactive dashboard was built to visualize:

- **Global KPIs**  
  - Total Transactions  
  - Total Amount Transacted  
  - Fraud Rate  
  - Total Fraud Transactions  
  - Average Fraud Amount  

- **Transaction Insights**  
  - Transaction Type Distribution  
  - Fraud Rate by Type  
  - Fraud Count by Hour of Day  

- **Risk Signal Analysis**  
  - Transactions by Drained Account Flag  
  - Total Transactions by Number of Triggered Flags  
  - Fraud Rate overlay per flag count  

- **Model Monitoring**  
  - Fraud Capture Rate by Decision (Allow / Review / Block)  
  - Fraud Rate inside each decision group  

### 🎨 Design Theme
- Dark background for analyst-style monitoring  
- Yellow for normal volume  
- Red for fraud risk indicators  
- Green for safe/allowed flows  

---

## 📈 Example Model Outcome
Fraud presence inside decisions:

| Decision | Fraud Rate |
|----------|------------|
| Block    | 42%        |
| Review   | 11%        |
| Allow    | 0.2%       |

**Meaning:**  
- Block queue → highly concentrated with fraud → good precision  
- Review queue → moderate fraud → analyst action justified  
- Allow queue → almost clean → low customer friction  

---

## 🧰 Tools Used
- **MySQL** — data cleaning, feature engineering, EDA  
- **Python (Pandas, Scikit-Learn, Matplotlib)** — modeling  
- **Power BI** — visualization and reporting  

---

## 🏁 Final Outcome
✔ Cleaned and engineered transaction dataset  
✔ Built fraud-predictive features  
✔ Trained high-performing anomaly model  
✔ Designed realistic decision thresholds  
✔ Implemented business decision layer  
✔ Created interactive monitoring dashboard  

This project demonstrates the full fraud detection pipeline from raw data to operational deployment simulation.

---

## 📚 Future Improvements
- Add external behavioral features (device, location, IP risk)  
- Use supervised classification instead of anomaly detection  
- Apply cost-based threshold optimization  
- Add transaction history aggregation features  
