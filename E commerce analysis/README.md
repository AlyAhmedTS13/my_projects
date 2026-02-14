# Customer Behavior Drift Detection — E-Commerce Segmentation & Analysis

> End-to-end data science project: synthetic data generation → SQL EDA & feature engineering → K-Means clustering → behavioral drift detection → statistical validation

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Problem](#business-problem)
- [Dataset](#dataset)
- [Project Architecture](#project-architecture)
- [Phase 1 — Synthetic Data Generation](#phase-1--synthetic-data-generation)
- [Phase 2 — SQL Exploratory Data Analysis](#phase-2--sql-exploratory-data-analysis)
- [Phase 3 — SQL Feature Engineering](#phase-3--sql-feature-engineering)
- [Phase 4 — K-Means Clustering Pipeline](#phase-4--k-means-clustering-pipeline)
- [Phase 5 — Cluster Profiling & Business Segmentation](#phase-5--cluster-profiling--business-segmentation)
- [Phase 6 — Behavioral Drift Analysis](#phase-6--behavioral-drift-analysis)
- [Phase 7 — Statistical Validation](#phase-7--statistical-validation)
- [Key Findings & Insights](#key-findings--insights)
- [Visualizations](#visualizations)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Future Improvements](#future-improvements)

---

## Project Overview

This project simulates a real-world scenario where an e-commerce company operating across the **Middle East** (Egypt, UAE, Saudi Arabia, Jordan) notices changes in customer behavior over time. The goal is to:

1. **Generate realistic synthetic data** with intentionally injected behavioral drift
2. **Explore the data** using SQL to understand business patterns
3. **Engineer ML-ready features** from raw transactional data using SQL
4. **Segment customers** using K-Means clustering
5. **Detect and analyze behavioral drift** over time across segments
6. **Statistically validate** that cluster differences are real

The project covers the full data science lifecycle — from raw data to actionable business insights.

---

## Business Problem

An e-commerce platform has 7,000 customers generating hundreds of thousands of events and orders over 18 months (July 2024 – December 2025). Starting around **Month 9 (March 2025)**, customer behavior begins to shift:

- Spending decreases
- Session durations drop
- Discount dependency increases
- Payment failure rates rise
- Browsing increases but purchasing decreases

**The question:** Can we detect this drift, segment customers into actionable groups, and provide data-driven recommendations?

---

## Dataset

Three tables were generated synthetically and loaded into **MySQL**:

| Table | Rows | Description |
|-------|------|-------------|
| `customers` | 7,000 | Customer demographics, signup info, account type |
| `events` | ~500,000 | Browsing sessions, logins, add-to-cart, purchases |
| `orders` | ~120,000 | Order transactions with payment status, discounts |

### Customers Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | INT | Unique identifier |
| `signup_date` | DATE | Account creation date |
| `birth_year` | INT | Year of birth |
| `gender` | VARCHAR | male / female / other |
| `country` | VARCHAR | Egypt / UAE / Saudi / Jordan |
| `region` | VARCHAR | Cairo, Giza, Alexandria, Delta, Gulf |
| `acquisition_channel` | VARCHAR | organic / paid_ads / referral |
| `device_preference` | VARCHAR | mobile / web |
| `account_type` | VARCHAR | free / premium |

### Events Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | INT | Unique event identifier |
| `customer_id` | INT | FK → customers |
| `event_timestamp` | DATETIME | When the event occurred |
| `event_date` | DATE | Date of event |
| `event_type` | VARCHAR | login / browse / add_to_cart / purchase |
| `session_id` | VARCHAR | Session identifier |
| `session_duration_sec` | INT | Session length in seconds |
| `page_category` | VARCHAR | home / search / product / checkout |
| `device_type` | VARCHAR | mobile / web |
| `app_version` | VARCHAR | v1.0 through v3.0 |
| `is_logged_in` | BOOLEAN | Whether user was logged in |
| `geo_country` | VARCHAR | Country of access |

### Orders Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | INT | Unique order identifier |
| `customer_id` | INT | FK → customers |
| `order_date` | DATE | Date of order |
| `order_value` | DECIMAL | Order amount in EGP |
| `currency` | VARCHAR | EGP |
| `items_count` | INT | Number of items |
| `payment_method` | VARCHAR | card / wallet / cash |
| `payment_status` | VARCHAR | success / failed / refunded |
| `discount_applied` | BOOLEAN | Whether discount was used |
| `discount_amount` | DECIMAL | Discount value in EGP |
| `product_category` | VARCHAR | electronics / fashion / groceries |
| `shipping_region` | VARCHAR | Shipping destination |

---

## Project Architecture

```
┌─────────────────────┐
│  Data Generation     │  Python (pandas, numpy)
│  (data generation    │  → 3 CSV files
│   .ipynb)            │  → Loaded into MySQL
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  SQL EDA             │  15+ business questions answered
│  (EDA E commerce     │  Revenue, engagement, conversion,
│   .sql)              │  drift patterns explored
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  Feature Engineering │  9 SQL views → 1 final table
│  (feature            │  20 ML-ready features:
│   enginerring.sql)   │  Monetary + Engagement + Recency + Volatility
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  ML Pipeline         │  K-Means Clustering (K=3)
│  (k means            │  Custom imputation → Z-score scaling
│   clustering.ipynb)  │  → Cluster profiling → Drift analysis
│                      │  → Statistical tests → Business insights
└─────────────────────┘
```

---

## Phase 1 — Synthetic Data Generation

**File:** `data generation.ipynb`

Generated realistic e-commerce data with controlled behavioral drift:

### Drift Mechanics

| Parameter | Pre-Drift (Months 1–8) | Post-Drift (Months 9+) |
|-----------|----------------------|----------------------|
| Session Duration | Normal (~600s mobile) | Decreases by up to 15% |
| Purchase Probability | 15–20% per session | Drops to ~8% |
| Order Value | Normal distribution | Decreases by up to 8% |
| Discount Usage | 25% base rate | Increases up to 60% |
| Payment Failure | 8% base rate | Increases by up to 4% |
| Browsing Behavior | Normal | More browse events per session |

### Drift Factor Formula

```
Month 1–8:   drift_factor = 0.0 (no drift)
Month 9–14:  drift_factor = 0.1 + (month - 9) × 0.12 (gradual)
Month 15+:   drift_factor = 1.0 (full drift)
```

### Customer Demographics Distribution

- **Countries:** Egypt 55%, UAE 20%, Saudi 18%, Jordan 7%
- **Gender:** Male 48%, Female 50%, Other 2%
- **Account Type:** Free 70%, Premium 30%
- **Device:** Mobile 72%, Web 28%
- **Channels:** Organic 45%, Paid Ads 35%, Referral 20%

---

## Phase 2 — SQL Exploratory Data Analysis

**File:** `EDA E commerce.sql`

15+ business questions investigated using SQL:

### Revenue & Financial Analysis
- **Total revenue by month** — tracked monthly revenue trends
- **Average order value by account type** — premium vs free comparison
- **Revenue by acquisition channel** — which channel brings highest revenue
- **Revenue per customer with discount trends** — discount impact on revenue
- **Revenue decline investigation** — fewer buyers vs lower spending per buyer

### Customer Engagement
- **Active customers per month** — monthly active user tracking
- **Session duration by device type** — mobile vs web engagement
- **Engagement changes after Month 9** — session duration and event count trends
- **Purchase conversion rate by month** — % of active users who purchase

### Conversion & Retention
- **Mobile vs web conversion rates** — device-based purchase rates
- **First purchase time** — avg days from signup to first order
- **Failed payment impact** — do failed payments reduce future purchases
- **6-month customer LTV by channel** — acquisition channel quality

### Drift Investigation
- **Engagement vs spending timeline for top 10 spenders** — do VIPs reduce engagement before spending
- **Regional engagement decline** — which regions show declining engagement
- **Discount dependency over time** — are discounts propping up revenue

### Key SQL EDA Findings

| Metric | Finding |
|--------|---------|
| Avg time to first purchase | Measured using window functions |
| Premium vs Free AOV | Premium users spend more per order |
| Failed payment customers | Still make ~12+ successful orders (resilient) |
| Mobile conversion | Lower than web despite higher traffic |
| Discount usage | Increased significantly post-drift |
| Top spenders | Engagement drops BEFORE spending drops |

---

## Phase 3 — SQL Feature Engineering

**File:** `feature enginerring.sql`

Built **9 SQL views** that feed into a final `ml_model_features` table with **20 ML-ready features** per customer:

### Feature Categories

#### Monetary Features (from `orders`)
```sql
CREATE VIEW Monetary_Features AS
SELECT customer_id,
    SUM(CASE WHEN payment_status = 'success' THEN order_value END) AS total_spent,
    SUM(CASE WHEN payment_status = 'success' THEN 1 ELSE 0 END) AS orders_count,
    AVG(CASE WHEN payment_status = 'success' THEN order_value END) AS avg_order_value,
    SUM(discount_applied)/COUNT(*) AS discount_usage_rate,
    SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END)/COUNT(*) AS failed_payment_rate,
    SUM(CASE WHEN payment_status = 'refunded' THEN 1 ELSE 0 END)/COUNT(*) AS refund_rate,
    AVG(CASE WHEN payment_status = 'success' THEN items_count END) AS avg_items_per_order
FROM orders GROUP BY customer_id;
```

#### Engagement Features (from `events`)
```sql
CREATE VIEW Engagement_Features AS
SELECT customer_id,
    COUNT(event_id) AS total_events,
    COUNT(DISTINCT event_date) AS active_days,
    AVG(session_duration_sec) AS avg_session_duration,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events,
    SUM(CASE WHEN event_type = 'browse' THEN 1 ELSE 0 END) AS browse_events
FROM events GROUP BY customer_id;
```

#### Recency Features
| View | Feature | Logic |
|------|---------|-------|
| `last_event` | `days_since_last_event` | `DATEDIFF(CURDATE(), MAX(event_date))` |
| `last_purchase` | `days_since_last_purchase` | `DATEDIFF(CURDATE(), MAX(order_date))` where success |
| `tenure_days` | `customer_tenure_days` | `DATEDIFF(CURDATE(), signup_date)` |

#### Volatility / Stability Features
| View | Feature | Logic |
|------|---------|-------|
| `customers_std` | `session_duration_std` | `STDDEV(session_duration_sec)` |
| `customers_std` | `order_value_std` | `STDDEV(order_value)` |
| `event_change` → `avg_event_change` | `avg_monthly_event_change` | Month-over-month event count change using `LAG()` |
| `spend_change` → `avg_spend_change` | `avg_monthly_spend_change` | Month-over-month spend change using `LAG()` |

#### Final Feature Table
All 9 views are `LEFT JOIN`ed onto the `customers` table to create `ml_model_features` — a single table with 7,000 rows and 28+ columns ready for ML.

---

## Phase 4 — K-Means Clustering Pipeline

**File:** `k means clustering.ipynb`

### Step 1: Data Loading
- Connected to MySQL and loaded `ml_model_features` table
- Selected 20 numeric features from `total_spent` onwards

### Step 2: Custom Imputation Strategy

Not all customers have orders (47% are browsers only), so NULLs have business meaning:

| Feature Group | Imputation | Reasoning |
|--------------|------------|-----------|
| Order metrics (`total_spent`, `orders_count`, `avg_order_value`, etc.) | Fill with **0** | No orders = zero value |
| `days_since_last_purchase` | Fill with **max + 1** | Never purchased = longest recency |
| `avg_monthly_event_change` | Fill with **median** | Only 13 nulls, median is robust |
| Std columns (`session_duration_std`, `order_value_std`) | Fill with **0** | No variance if no orders |

### Step 3: Z-Score Scaling (StandardScaler)
- Transforms all features to mean=0, std=1
- Ensures K-Means doesn't favor high-magnitude features (e.g., `total_spent` in thousands vs `discount_usage_rate` as 0–1)

### Step 4: Optimal K Selection

Tested K=2 through K=10 using:
- **Elbow Method** (inertia / within-cluster sum of squares)
- **Silhouette Score** (cluster separation quality)

| K | Silhouette Score | Notes |
|---|-----------------|-------|
| 2 | 0.38 (highest) | Too simple — only 2 groups |
| **3** | **0.33** | **Chosen — 3 actionable segments** |
| 4 | ~0.30 | Two clusters overlap, redundant |

**Why K=3 over K=2:** Despite K=2 having a slightly higher silhouette score, K=3 provides three distinct business segments (Non-Buyers, Regular Buyers, VIP). K=4 was tested and produced two nearly identical clusters. **Business interpretability was prioritized over a marginal score difference.**

### Step 5: K-Means Training
```python
OPTIMAL_K = 3
kmeans_final = KMeans(n_clusters=3, random_state=42, n_init=10)
cluster_labels = kmeans_final.fit_predict(X_scaled)
```

---

## Phase 5 — Cluster Profiling & Business Segmentation

### Segment Summary

| Cluster | Segment Name | Customers | % of Total | Avg Spend (EGP) | Avg Orders | % Revenue | Zero-Spend % |
|---------|-------------|-----------|-----------|-----------------|------------|-----------|-------------|
| 0 | Active Browsers (Non-Buyers) | 3,319 | 47.4% | 0 (NaN) | 0 | 0% | 100% |
| 1 | VIP / High-Value Loyal | 1,448 | 20.7% | 38,740 | 36.3 | 54% | 0% |
| 2 | Regular Active Buyers | 2,233 | 31.9% | 21,371 | 24.1 | 46% | 0% |

### Segment Logic (Fixed)

The segment naming uses explicit zero-spend checking rather than relying on mean thresholds:

```python
if pct_zero_spend > 0.9:    # 90%+ have $0 spent
    if avg_sessions > 10:
        → "Active Browsers (Non-Buyers)"
    else:
        → "Inactive / Low Engagement"
elif avg_spent > 25000 and avg_recency < 450:
    → "VIP / High-Value Loyal"
elif avg_spent > 15000 and avg_recency < 500:
    → "Regular Active Buyers"
```

### Pareto Analysis (80/20 Rule)
- **20.7%** of customers (VIP) generate **54%** of revenue
- **31.9%** of customers (Regular) generate **46%** of revenue
- **47.4%** of customers (Browsers) generate **0%** of revenue

### Customer Lifetime Value (CLV)

| Cluster | Avg Annual CLV | Purchases/Month |
|---------|---------------|-----------------|
| VIP | 27,029 EGP | 2.11 |
| Regular | 14,778 EGP | 1.39 |
| Browsers | 0 EGP | 0 |

### Demographic Analysis

| Dimension | Finding | Business Implication |
|-----------|---------|---------------------|
| **Gender** | ~Equal across all 3 clusters (Female slightly more) | Gender doesn't drive segmentation |
| **Country** | Egypt dominant (~55%), UAE & Saudi ~equal, Jordan smallest | Consistent market distribution |
| **Account Type** | Premium far more common in VIP cluster; Free dominates others | Account type IS a differentiator |
| **Acquisition Channel** | Same order (Organic > Ads > Referral) across all clusters | Channel doesn't predict customer value |

### VIP Migration Risk Analysis

Calculated **Risk Ratio** = Days Since Last Purchase / Expected Purchase Gap:

| Cluster | Expected Gap | Actual Recency | Risk Ratio | Status |
|---------|-------------|----------------|------------|--------|
| VIP | 14.3 days | 403 days | 28.2x | CRITICAL |
| Regular | 21.7 days | 447 days | 20.6x | CRITICAL |

Both buying clusters show customers significantly overdue for purchases — a direct result of the behavioral drift.

---

## Phase 6 — Behavioral Drift Analysis

### Time-Series Drift Visualization

6 metrics tracked monthly across all 3 clusters with drift start marked at March 2025:

1. **Monthly Spending** — VIP spending drops from ~28K to ~15K EGP
2. **Discount Usage** — Increases from ~25% to ~55% across all segments
3. **Session Duration** — Decreases post-drift, especially for mobile users
4. **Active Customers** — Declining customer activity month over month
5. **Average Order Value** — Gradual decrease across segments
6. **Engagement Events** — Browsing increases while purchasing decreases

### Pre vs Post Drift Comparison

| Cluster | Pre-Drift Spend | Post-Drift Spend | Change | Discount Change |
|---------|----------------|------------------|--------|----------------|
| VIP | Higher | Lower | **-29%** | **+14.9%** |
| Regular | Higher | Lower | **-14.1%** | **+7.8%** |

### Automatic Drift Detection (Without Prior Knowledge)

Three statistical methods implemented for real-world drift detection:

| Method | What It Detects | Threshold |
|--------|----------------|-----------|
| **Rolling Z-Score** | Month-by-month anomalies | Z > 1.5 = drift point |
| **Population Stability Index (PSI)** | Distribution shift between periods | PSI > 0.25 = significant |
| **Kolmogorov-Smirnov Test** | Statistical distribution difference | p < 0.05 = confirmed drift |

---

## Phase 7 — Statistical Validation

### ANOVA Test (Numeric Features)

All 8 tested numeric features showed **statistically significant** differences between clusters (p < 0.05):

| Feature | F-Statistic | P-Value | Significant? |
|---------|------------|---------|-------------|
| `total_spent` | High | < 0.001 | Yes |
| `orders_count` | High | < 0.001 | Yes |
| `avg_order_value` | High | < 0.001 | Yes |
| `days_since_last_purchase` | High | < 0.001 | Yes |
| `avg_session_duration` | High | < 0.001 | Yes |
| `discount_usage_rate` | High | < 0.001 | Yes |
| `total_events` | Very High | < 0.001 | Yes |
| `purchase_events` | Very High | < 0.001 | Yes |

### Chi-Square Test (Categorical Features)

| Feature | Cramer's V | Effect Size | Significant? |
|---------|-----------|-------------|-------------|
| `account_type` | **0.44** | **Large** | Yes |
| `country` | 0.04 | Small | Yes (but weak) |
| `gender` | 0.03 | Small | Marginal |
| `acquisition_channel` | 0.02 | Small | Marginal |

**Key Takeaway:** `account_type` is the only categorical feature with a **large effect** on cluster membership. Demographics (gender, country, channel) show small/negligible effects — confirming that **behavior, not demographics, drives segmentation**.

### Mann-Whitney U Pairwise Tests

All pairwise cluster comparisons (C0 vs C1, C0 vs C2, C1 vs C2) are **statistically significant** for key features — confirming each cluster is genuinely distinct.

---

## Key Findings & Insights

### 1. Customer Segmentation Works
- K-Means with K=3 produces clear, actionable segments
- Clusters are statistically validated (ANOVA + Chi-Square + Mann-Whitney)

### 2. Behavioral Drift is Real and Measurable
- VIP spending dropped **29%** post-drift
- Discount dependency increased **15%** for VIP customers
- All clusters affected, but VIPs show the steepest decline

### 3. Demographics Don't Drive Value
- Gender, country, and channel are consistent across clusters
- **Account type** (premium vs free) is the strongest categorical predictor

### 4. 47% of Customers Never Buy
- 3,319 customers (47.4%) are active browsers with zero purchases
- Massive conversion opportunity — first-purchase incentives could unlock revenue

### 5. VIP Recency Risk is Critical
- VIP customers are 28x overdue for their expected purchase gap
- Immediate retention campaigns needed

### Business Recommendations

| Segment | Action | Priority |
|---------|--------|----------|
| **VIP** | VIP loyalty program, exclusive offers, personal account manager | Critical |
| **Regular** | Cross-sell campaigns, loyalty rewards, upsell to premium | High |
| **Browsers** | First-purchase incentive (10% off), cart recovery emails, social proof | High |
| **All** | Reduce discount dependency gradually, A/B test pricing | Medium |

---

## Visualizations

The notebook generates 20+ visualizations including:

- **Elbow & Silhouette plots** — K selection
- **Cluster heatmap** — Feature comparison across segments
- **Boxplots** — Feature distributions per cluster
- **PCA 2D projection** — Cluster separation visualization (63% variance explained)
- **Radar chart** — Normalized cluster profiles
- **Demographic bar charts** — Country, gender, account type, channel by cluster
- **Revenue pie charts** — Customer vs revenue distribution
- **Executive dashboard** — Combined 6-panel + summary table
- **Drift time-series** — 6 metrics tracked monthly with drift line
- **Pre vs post drift bars** — Spending and discount comparison
- **Risk distribution** — VIP recency risk histogram
- **ANOVA & Chi-Square charts** — Statistical significance visualization

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Generation | Python (pandas, numpy) |
| Database | MySQL |
| EDA | SQL (15+ queries with JOINs, CTEs, window functions) |
| Feature Engineering | SQL (views, LAG, STDDEV, CASE WHEN, DATEDIFF) |
| ML Pipeline | scikit-learn (KMeans, StandardScaler, PCA, silhouette_score) |
| Statistical Tests | scipy.stats (ANOVA, Chi-Square, Mann-Whitney U, KS Test) |
| Visualization | matplotlib, seaborn |
| Environment | Jupyter Notebook (VS Code), Python 3.x |

---

## Project Structure

```
E commerce/
├── README.md                      # This file
├── data generation.ipynb          # Synthetic data generation (3 CSV outputs)
├── EDA E commerce.sql             # 15+ SQL exploratory queries
├── feature enginerring.sql        # 9 SQL views → ml_model_features table
└── k means clustering.ipynb       # Full ML pipeline (23 steps)
```

### Notebook Steps (k means clustering.ipynb)

| Step | Description |
|------|-------------|
| 1 | Data loading from MySQL |
| 2 | Custom imputation function |
| 3 | Z-score scaling (StandardScaler) |
| 4 | Optimal K search (Elbow + Silhouette) |
| 5 | K-Means training (K=3) |
| 6 | Cluster profiling (mean & median) |
| 7 | Business segment naming (fixed: explicit zero-spend check) |
| 8 | Revenue & customer distribution |
| 9 | Cluster heatmap |
| 10 | Feature boxplots |
| 11 | PCA 2D visualization |
| 12 | Radar chart |
| 13 | Demographic analysis |
| 14 | Strategic recommendations |
| 15 | Customer Lifetime Value (CLV) |
| 16 | Feature importance per cluster |
| 17 | Executive summary dashboard |
| 18 | Drift data loading (monthly SQL queries) |
| 19 | Drift time-series visualization (6 charts) |
| 20 | Pre vs post drift comparison |
| 21 | Automatic drift detection (Z-Score, PSI, KS Test) |
| 22 | VIP migration / recency risk analysis |
| 23 | Statistical significance tests (ANOVA, Chi-Square, Mann-Whitney) |

---

## How to Run

### Prerequisites
- Python 3.8+
- MySQL Server
- Required packages: `pandas`, `numpy`, `scikit-learn`, `matplotlib`, `seaborn`, `scipy`, `mysql-connector-python`

### Steps

1. **Generate data:**
   ```bash
   # Run data generation.ipynb
   # Outputs: customers.csv, events.csv, orders.csv
   ```

2. **Load into MySQL:**
   ```sql
   CREATE DATABASE e_commerce;
   -- Import the 3 CSV files into tables: customers, events, orders
   ```

3. **Run EDA:**
   ```bash
   # Execute queries in EDA E commerce.sql
   ```

4. **Build features:**
   ```bash
   # Execute feature enginerring.sql
   # Creates ml_model_features table
   ```

5. **Run ML pipeline:**
   ```bash
   # Run k means clustering.ipynb (all 23 steps)
   ```

---

## Future Improvements

- **Real-time scoring pipeline** — Score new customers into segments automatically
- **Power BI / Tableau dashboard** — Interactive business reporting
- **A/B testing framework** — Test retention strategies per segment
- **Automated re-clustering** — Periodic model retraining when drift is detected
- **Deep learning approach** — LSTM/autoencoder for temporal drift detection
- **Additional algorithms** — Compare K-Means with DBSCAN, Gaussian Mixture Models

---

## Author

Built as a comprehensive data science portfolio project demonstrating:
- SQL proficiency (EDA + feature engineering with views, window functions, CTEs)
- Python ML pipeline design (custom imputation, scaling, clustering)
- Statistical rigor (ANOVA, Chi-Square, Mann-Whitney, PSI, KS tests)
- Business acumen (segment naming, CLV, strategic recommendations)
- Data storytelling (20+ visualizations, executive dashboard)
