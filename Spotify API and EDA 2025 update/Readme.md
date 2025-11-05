# 🎧 Spotify Metadata Pipeline: From API to Cleaned Dataset & EDA

## 📌 Overview  
This project builds a complete pipeline to extract, clean, and analyze music metadata from Spotify using **Python**, **SQL**, and **Power BI**. Beyond technical execution, the project was designed to explore key questions about the **music industry and genre trends in 2025**, including artist dominance, release patterns, and the evolution of popular genres.

---

## 🛠️ Tools & Technologies  
- **Spotify API** (OAuth + Client Credentials)  
- **Python** (`requests`, `pandas`, `Flask`)  
- **SQL** (MySQL for cleaning and enrichment)  
- **Power BI** (for interactive dashboards and EDA)

---

## 🔄 Pipeline Breakdown

### 1. 🎯 Data Collection (Python + Spotify API)
- Authenticated using **personal OAuth credentials** to access private and collaborative playlists
- Extracted metadata from **hundreds of playlists and albums** (full list included)
- Validated playlist/album IDs and filtered out private or inaccessible items
- Fetched over **8,700 unique tracks** with metadata including:
  - Track name, artist name, album name
  - Track popularity, album release date, album type
  - Artist followers, popularity, genres

### 2. 🧠 Resilience & Reliability
- Added `sleep()` intervals to **prevent server timeouts**
- Saved progress every **10 tracks** to a CSV file
- Created **backup files** to recover from connection drops or crashes
- Code ran for **several hours** to complete full extraction
- Some playlists were private or removed, and were excluded from final results

---

## 🧹 Data Cleaning (SQL)

Performed extensive cleaning and enrichment using SQL:

- Removed nulls and standardized string formats
- Trimmed whitespace and uppercased flags like `explicit`
- Normalized `album_release_date` from year-only or year-month formats to full dates
- Converted `track_duration_ms` to minutes
- Cleaned `artist_genres` using regex and manually populated missing genres for key artists
- Removed duplicates using `ROW_NUMBER()` logic
- Final cleaned table: `spotify_api_clean`

---

## 📊 Exploratory Data Analysis (SQL + Python)

### 🔍 SQL Highlights:
- Top artists by followers  
- Explicit vs. clean track distribution  
- Album type breakdown  
- Track release trends by year  
- Longest and shortest tracks  
- Pop genre dominance  
- Artist popularity vs. track success

### 🐍 Python Highlights:
- Used pandas to **explode multi-genre strings** into individual tags
- Normalized genre labels with `.str.strip().str.lower()`
- Counted genre frequency with `value_counts()`
- Chose Python over SQL for genre analysis due to:
  - Easier syntax
  - More accurate handling of nested strings
  - Avoided complex recursive CTEs

---

## 📈 Power BI Dashboards

Built interactive visuals that **mirror the logic used in SQL and Python**:

- Replicated genre logic using **Power Query**:
  - Split `artist_genres` by comma → into rows
  - Trimmed whitespace
- Used DAX, calculated columns, and native tools to match backend logic
- Ensured all graphs **align with SQL and pandas EDA**

### 📊 Visuals Include:
- Tracks released per year
- Top genres by count
- Pop genre dominance by artist
- Album type distribution
- Explicit vs. clean track ratio
- Top tracks by popularity
- Top artists by followers

---

## 📁 Files Included
- `spotify_data.csv`: Raw extracted data  
- `tracks_final.csv`: Final cleaned dataset  
- `playlist_IDs.txt`: Full list of Spotify sources  
- Power BI dashboard screenshots  
- SQL cleaning and EDA scripts  
- Python API extraction and genre analysis notebooks

---

## 🤖 ML Potential
This dataset is ready for machine learning tasks such as:
- Predicting track popularity based on metadata
- Recommending songs based on genre and artist metrics
- Clustering tracks by release patterns and artist traits

---

## 📥 Access the Data
- [Kaggle Dataset](https://www.kaggle.com/datasets/alyahmedts13/spotify-songs-for-ml-and-analysis-over-8700-tracks)

---

## 💬 Let’s Connect
If you’re into music analytics, ML, or data storytelling — I’d love to hear your thoughts or collaborate on future projects.
