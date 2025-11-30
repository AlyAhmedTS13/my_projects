# 🚗 Used Car Price Prediction in Egypt (End-to-End Data Project)

## 📌 Executive Summary
This project is a complete, end-to-end data science lifecycle designed to predict used car prices in the Egyptian market. We engineered a pipeline that starts with extracting raw data from **OLX Egypt (Dubizzle)**, moves through rigorous cleaning and feature engineering, leverages **SQL** and **Power BI** for deep market insights, and culminates in a deployed **XGBoost** machine learning model.

The final model achieves a predictive accuracy where the error margin is approximately **12% on average**, providing a reliable tool for valuing vehicles in a highly volatile market.

---

## 🛠️ Tech Stack & Tools

* **Data Acquisition:** Python (`BeautifulSoup`, `requests`), **FlareSolverr** (Anti-bot bypass).
* **Data Processing:** `Pandas`, `NumPy`, `Regular Expressions (Re)`.
* **Database:** **MySQL** (Structured storage and analytical querying).
* **Visualization:** **Power BI** (Interactive dashboards), `Seaborn`/`Matplotlib` (Statistical plots).
* **Machine Learning:** `Scikit-Learn` (Pipelines, Preprocessing), `XGBoost` (Regressor), `Joblib` (Model serialization).

---

## 🔄 Project Workflow (Sequential)

The project followed a strict linear pipeline to ensure data integrity:

1.  **Web Scraping:** Harvesting raw data.
2.  **Data Cleaning & Feature Engineering:** Transforming mess into metrics.
3.  **EDA (SQL & Power BI):** Analyzing the *clean* data for insights.
4.  **Machine Learning:** Modeling and evaluation.

---

## 1️⃣ Phase 1: Advanced Web Scraping
**Goal:** Build a custom dataset from scratch rather than relying on stale Kaggle datasets.

### The Challenge: Anti-Bot Protection
The target site (OLX/Dubizzle) uses sophisticated Cloudflare-style protection that blocks standard HTTP requests.
* **Solution:** We implemented **FlareSolverr**, a proxy server that solves JavaScript challenges (CAPTCHAs) automatically. This allowed our Python script to establish a stable session and retrieve HTML content without being flagged as a bot.

### The Logic
1.  **Pagination Loop:** We iterated through **200 pages** of listings to gather a wide breadth of data.
2.  **Link Extraction:** We first harvested **8,519 unique URLs** to ensure we didn't waste resources scraping duplicates.
3.  **Details Scraping:** A secondary script visited each unique URL to extract granular attributes: *Brand, Model, Year, Kilometers, Transmission, Body Type, Engine CC,* and most importantly, the unstructured *Description* text.
4.  **Checkpointing:** To prevent data loss during network interruptions, the script saved progress to a CSV file every 2 links.

---

## 2️⃣ Phase 2: Data Cleaning & Feature Engineering
**Goal:** Transform raw, noisy web data into a structured dataset ready for analysis.

### Cleaning Strategy: Two-Layered Outlier Removal
We implemented a strategic, two-step approach to handling outliers to ensure the data represented the true market:

1.  **Step 1: Domain-Specific Range Filtering:**
    We first applied broad "sanity check" filters based on domain knowledge (e.g., Price 100k - 7M EGP).
    * *Why?* Raw web data often contains massive data entry errors (e.g., 1 EGP or 100M EGP). If we applied statistical methods like IQR immediately on this raw data, the upper and lower bands would be artificially wide and ineffective.
2.  **Step 2: Statistical IQR Filtering:**
    After setting the extremes, we applied the **Interquartile Range (IQR)** method.
    * *Result:* This refined the dataset strictly, bringing the **Maximum Price to ~2.6 Million EGP** and the **Mean Price to ~711,000 EGP**. This removed the ultra-luxury outliers to ensure the model focuses on the core market.

### Feature Engineering: The "Description" Goldmine
The raw `Description` column contained valuable details hidden in unstructured Arabic and English text. We used **Regular Expressions (Regex)** to mine this text and create **41 new binary features**.

* **Why is this critical?** In the Egyptian market, price variance is not driven by *Year* and *Model* alone. Seller keywords are major price determinants.
* **Key Insight:** A car listed as **"Fabrika"** (factory paint) vs. one listed with **"Rasha"** (repainted) or **"Accident"** can have a price difference of 20-30% for the exact same model year.
* **Outcome:** By extracting these nuances (along with luxury features like `Sunroof` or `Screen`), we enabled our EDA to visualize these specific price gaps and allowed the model to "see" these subtle market drivers.

---

## 3️⃣ Phase 3: Exploratory Data Analysis (SQL & Power BI)
**Goal:** Use the *cleaned* data to understand market dynamics before feeding it to the model.

### 🧠 Part A: SQL Analysis
We imported the clean CSV into **MySQL** to run complex aggregations. We didn't just look at averages; we segmented the data to answer specific business questions:
1.  **Depreciation Analysis:** We calculated the average price drop per 100,000km to understand how mileage kills value.
2.  **Segmenting Engine Size:** We created custom SQL bins (Small <1300cc, Medium 1300-1600cc, Large >2000cc) to see which segment holds value best.
3.  **Value Retention:** We queried which Brands/Models have the highest average resale price, identifying brands like **Porsche** and **Jeep** as value leaders, while identifying widespread availability for brands like **Hyundai** and **Chevrolet**.

### 📊 Part B: Power BI Visualization
We connected Power BI to the dataset to visualize the SQL findings:
* **Trend Analysis (Line Charts):** Visualized the exponential relationship between *Year* and *Price*. The curve becomes steep post-2015, indicating massive inflation on newer models.
* **Feature Impact (Bar Charts):** We calculated the "Premium Percentage". For example, cars with a **Sunroof** sold for significantly higher averages than those without, validating the importance of our Regex feature engineering.
* **Market Composition (Donut Charts):** Showed that the market is dominated by **Sedans**, but SUVs are the fastest-growing segment in terms of value.

---

## 4️⃣ Phase 4: Machine Learning & Deployment
**Goal:** Build a predictive model that generalizes well to unseen data.

### The Pipeline Architecture
We avoided manual preprocessing to prevent **data leakage**. Instead, we built a robust Scikit-Learn `Pipeline`:

1.  **Target Transformation (`log1p`):**
    * *Problem:* Car prices are highly right-skewed (mostly cheap cars, few very expensive ones). This confuses models.
    * *Solution:* We trained the model on the `log(Price)` and then exponentiated the result (`np.expm1`) for the final prediction. This normalized the error distribution.

2.  **Column Transformer:**
    * **Categorical (Brand, Model, Type):** Applied `OneHotEncoder`. We handled "unknown" categories (models not seen in training) by encoding them as all-zeros to prevent crashes in production.
    * **Numerical (CC, Km):** Applied `LogTransform` because mileage and engine size also follow power-law distributions.
    * **Binary (Sunroof, Accident, etc.):** Passed through as-is.

### Model Selection: XGBoost
We chose **XGBoost Regressor** over Linear Regression or Random Forest because:
* It handles missing values natively (though we cleaned them).
* It captures **non-linear relationships** (e.g., depreciation slows down as a car gets very old).
* It is highly resistant to overfitting when tuned correctly.

### Results & Evaluation
* **Testing MAE:** ~85,000 EGP
* **Average Error Percentage:** ~12%
* **Conclusion:** The model's predictions deviate by roughly **12%** on average. Considering the high volatility of the Egyptian used car market—where prices for the same car can vary wildly based on seller temperament and condition—this performance is considered highly effective for real-world estimation.

---