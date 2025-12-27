# 🎬 CineMatch - Movie Recommendation System

<div align="center">

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white)
![Django](https://img.shields.io/badge/Django-6.0-green?logo=django&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-orange?logo=scikit-learn&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-Database-003B57?logo=sqlite&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-F2C811?logo=powerbi&logoColor=black)

**A full-stack movie recommendation web application powered by machine learning, featuring 95,000+ movies with personalized recommendations, intelligent search, and beautiful analytics.**

[Features](#-features) • [Tech Stack](#-tech-stack) • [Data Pipeline](#-data-pipeline) • [ML Engine](#-the-machine-learning-engine) • [Website](#-the-django-web-application) • [Analytics](#-exploratory-data-analysis)

</div>

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Features](#-features)
3. [Tech Stack](#-tech-stack)
4. [Data Pipeline](#-data-pipeline)
   - [Phase 1: Data Collection](#phase-1-data-collection)
   - [Phase 2: Data Merging](#phase-2-data-merging)
   - [Phase 3: Data Cleaning](#phase-3-data-cleaning)
5. [The Machine Learning Engine](#-the-machine-learning-engine)
   - [Initial Approach: Cosine Similarity](#initial-approach-cosine-similarity-matrix)
   - [Final Solution: Nearest Neighbors](#final-solution-nearest-neighbors-algorithm)
   - [Feature Engineering](#feature-engineering)
6. [The Django Web Application](#-the-django-web-application)
   - [Architecture Overview](#architecture-overview)
   - [Search & Discovery](#search--discovery-system)
   - [Personalized "For You" Recommendations](#personalized-for-you-recommendations)
   - [User Interactions](#user-interactions)
7. [Exploratory Data Analysis](#-exploratory-data-analysis)
   - [SQL Analysis](#sql-analysis)
   - [Power BI Visualizations](#power-bi-visualizations)
8. [Database Schema](#-database-schema)
9. [Project Structure](#-project-structure)
10. [Setup & Installation](#-setup--installation)
11. [Future Improvements](#-future-improvements)

---

## 🎯 Project Overview

CineMatch is an end-to-end movie recommendation system that combines:

- **Data Engineering**: Collecting and processing 95,000+ movies from multiple sources
- **Machine Learning**: Content-based filtering using TF-IDF and K-Nearest Neighbors
- **Web Development**: A modern Django application with a sleek "Neon Cinema" UI
- **Data Analytics**: SQL queries and Power BI dashboards for business insights

The project demonstrates the complete data science workflow from raw data collection to a deployed web application with real-time recommendations.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🎬 **95,445 Movies** | Comprehensive database spanning from 1874 to 2025 |
| 🤖 **Smart Recommendations** | ML-powered similar movie suggestions using K-NN |
| 👤 **Personalized "For You"** | Recommendations based on your liked movies |
| 🔍 **Intelligent Search** | Multi-type search with fuzzy matching fallback |
| ❤️ **User Interactions** | Like, Watchlist, and Review functionality |
| 📊 **Rich Analytics** | SQL and Power BI insights on movie data |
| 🎨 **Modern UI** | Glassmorphism design with neon accents |
| ⚡ **Optimized Performance** | Local DB caching with smart API fallback |

---

## 🛠 Tech Stack

### Data Science & ML
- **Python 3.13** - Core programming language
- **Pandas & NumPy** - Data manipulation and analysis
- **scikit-learn** - Machine learning (TF-IDF, NearestNeighbors)
- **RapidFuzz** - Fuzzy string matching for search
- **Joblib** - Model serialization

### Web Development
- **Django 6.0** - Backend web framework
- **SQLite3** - Production database
- **Bootstrap 5.3** - Frontend framework
- **HTML/CSS/JavaScript** - Custom UI components

### Data & APIs
- **TMDB API** - Real-time movie data and images
- **MovieLens Dataset** - Source of TMDB IDs for collection
- **BeautifulSoup** - Web scraping for additional data

### Analytics
- **MySQL** - SQL analysis queries
- **Power BI** - Interactive dashboards and visualizations

---

## 📊 Data Pipeline

The data pipeline transforms raw movie IDs into a clean, analysis-ready database. This multi-phase process ensures data quality and completeness.

### Phase 1: Data Collection

**Source: MovieLens → TMDB API**

We started with the [MovieLens 32M dataset](https://grouplens.org/datasets/movielens/), which provides a `links.csv` file containing TMDB IDs for thousands of movies. These IDs became our gateway to collecting rich movie metadata.

```python
# movies data set.ipynb - API Collection Pipeline

TMDB_API_KEY = "your_api_key"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_tmdb_movie_data(tmdb_id):
    """Fetches comprehensive movie data from TMDB API"""
    
    # 1. Base movie data (title, overview, release_date, runtime, etc.)
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    response = requests.get(url, params={"api_key": TMDB_API_KEY})
    data = response.json()
    
    # 2. Keywords endpoint for thematic tags
    keywords_url = f"{TMDB_BASE_URL}/movie/{tmdb_id}/keywords"
    keywords = requests.get(keywords_url, params=params).json().get("keywords", [])
    
    # 3. Credits endpoint for cast and crew
    credits_url = f"{TMDB_BASE_URL}/movie/{tmdb_id}/credits"
    credits = requests.get(credits_url, params=params).json()
    cast = [member["name"] for member in credits.get("cast", [])[:5]]  # Top 5 actors
    director = next((m["name"] for m in credits.get("crew", []) if m["job"] == "Director"), None)
    
    return {
        "movie_id": tmdb_id,
        "title": data.get("title"),
        "overview": data.get("overview"),
        "genres": [genre["name"] for genre in data.get("genres", [])],
        "keywords": [kw["name"] for kw in keywords],
        "cast": cast,
        "director": director,
        "release_date": data.get("release_date"),
        "runtime": data.get("runtime"),
        "popularity": data.get("popularity"),
        "poster_url": TMDB_IMAGE_BASE_URL + data["poster_path"] if data.get("poster_path") else None
    }
```

**Backup & Resume System**: The collection process saves a backup every 10 movies, allowing the script to resume from where it left off if interrupted:

```python
# Save backup every 10 movies
if (idx + 1) % 10 == 0:
    temp_df = pd.DataFrame(results)
    movies_df = pd.concat([movies_df, temp_df], ignore_index=True)
    movies_df.to_csv(BACKUP_FILE, index=False)
    results = []
```

**Rate Limiting**: We respect TMDB's API limits with a 0.25-second delay between requests.

### Phase 2: Data Merging

**Combining Multiple Data Sources**

The data came from three separate collection runs, each capturing different movie attributes:

```python
# data merge.ipynb - Merging Pipeline

# Load the two main datasets
df1 = pd.read_csv("tmdb_movies_backup.csv")      # Core movie data
df2 = pd.read_csv("tmdb_movies_extra_backup.csv") # Extended data (cast with profiles)

# Handle overlapping columns (keep better cast data from df2)
df1 = df1.drop(columns=['cast'])

# Merge on movie_id (inner join to keep only complete records)
merged_df = pd.merge(df1, df2, on='movie_id', how='inner', suffixes=('_left', '_right'))

# Add modern movies (2024-2025 releases)
df3 = pd.read_csv("modern_movies_2024_2025.csv")
df3.drop_duplicates(inplace=True)

# Concatenate all sources
final_df = pd.concat([merged_df, df3], ignore_index=True)
final_df.to_csv("movies_DB.csv", index=False)
```

**Result**: A unified dataset of **95,445 unique movies** with consistent schema.

### Phase 3: Data Cleaning

**Transforming Raw Data into Production-Ready Format**

```python
# db_clean.ipynb - Comprehensive Data Cleaning

import pandas as pd
import json

df = pd.read_csv('movies_DB.csv')

# ═══════════════════════════════════════════════════════════════
# STEP 1: Remove Irrelevant Columns
# ═══════════════════════════════════════════════════════════════
# Videos and images will be fetched fresh from API on demand
# Review texts were collected but not needed for recommendations
df.drop(columns=['video_keys', 'review_texts', 'poster_urls'], inplace=True)

# ═══════════════════════════════════════════════════════════════
# STEP 2: Data Type Conversion
# ═══════════════════════════════════════════════════════════════
# Convert release_date string to proper datetime
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

# Convert JSON strings to Python objects (lists/dicts)
list_cols = ['genres', 'keywords', 'cast']
for col in list_cols:
    df[col] = df[col].apply(lambda x: eval(x) if pd.notnull(x) else x)

dict_cols = ['external_ids']
for col in dict_cols:
    df[col] = df[col].apply(lambda x: eval(x) if pd.notnull(x) else x)

# ═══════════════════════════════════════════════════════════════
# STEP 3: Handle Missing Values (Strategic Approach)
# ═══════════════════════════════════════════════════════════════
df['overview'] = df['overview'].fillna("No overview available")  # Readable fallback
df['tagline'] = df['tagline'].fillna("")                          # Blank if none
df['director'] = df['director'].fillna("Unknown")                 # Explicit unknown
df['certification'] = df['certification'].fillna("N/A")           # Standard notation
df['poster_url'] = df['poster_url'].fillna("")                    # Empty for placeholder logic

# Note: release_date NaT values are handled at display time in Django templates

# ═══════════════════════════════════════════════════════════════
# STEP 4: Remove Duplicates
# ═══════════════════════════════════════════════════════════════
df.drop_duplicates(subset=['movie_id'], inplace=True)

# ═══════════════════════════════════════════════════════════════
# STEP 5: Export with JSON Serialization
# ═══════════════════════════════════════════════════════════════
# Convert complex types back to JSON strings for CSV storage
list_dict_cols = ['genres', 'keywords', 'cast', 'external_ids']
for col in list_dict_cols:
    df[col] = df[col].apply(lambda x: json.dumps(x))

df.to_csv('movies_DB_cleaned.csv', index=False)
```

**Data Quality Summary**:

| Metric | Before Cleaning | After Cleaning |
|--------|-----------------|----------------|
| Total Records | 95,500+ | 95,445 |
| Duplicate Movies | 55+ | 0 |
| Missing Overviews | ~200 | 0 (filled) |
| Unknown Directors | ~5,000 | Labeled "Unknown" |
| Invalid Dates | ~150 | Preserved as NaT |

---

## 🤖 The Machine Learning Engine

### Initial Approach: Cosine Similarity Matrix

Our first instinct was to use a **cosine similarity matrix**, which is the standard approach for content-based recommendation:

```python
from sklearn.metrics.pairwise import cosine_similarity

# Compute full similarity matrix
similarity_matrix = cosine_similarity(tfidf_matrix)
# This creates an n × n matrix where n = number of movies
```

**The Problem**: For our 95,445-movie dataset, this would create a matrix with:
- **9.1 BILLION cells** (95,445 × 95,445)
- Approximately **68 GB of memory** (at 8 bytes per float)

This approach is completely infeasible for datasets of this scale.

### Final Solution: Nearest Neighbors Algorithm

We solved the memory problem using **scikit-learn's NearestNeighbors**, which computes similarities on-demand:

```python
# recommendation system.ipynb

from sklearn.neighbors import NearestNeighbors
from sklearn.feature_extraction.text import TfidfVectorizer

# 1. Create TF-IDF vectorizer (converts text to numerical features)
tfidf = TfidfVectorizer(stop_words='english')
tfidf_matrix = tfidf.fit_transform(df['combined_features'])

# 2. Build Nearest Neighbors model with cosine metric
nn_model = NearestNeighbors(metric='cosine', algorithm='brute')
nn_model.fit(tfidf_matrix)

# 3. Query for recommendations (only computes when needed!)
distances, indices = nn_model.kneighbors(
    tfidf_matrix[movie_idx],    # Query vector for one movie
    n_neighbors=11               # Get top 10 + the movie itself
)
```

**Why This Works**:
- **Memory**: Only stores the TF-IDF matrix (~500MB) instead of the full similarity matrix
- **Speed**: Computes distances on-the-fly for only the movies we need
- **Accuracy**: Uses the same cosine distance metric, producing identical results

### Feature Engineering

The recommendation quality depends heavily on how we represent each movie. We created a **combined_features** column:

```python
def join_names_from_cast(cast):
    """Extract top 5 actor names from cast list"""
    list_cast = []
    for member in cast[:5]:
        if 'name' in member:
            list_cast.append(member['name'])
    return " ".join(list_cast)

# Combine all relevant text features
df['combined_features'] = (
    df['overview'] + " " +           # Plot description
    df['tagline'] + " " +            # Marketing tagline
    df['genres'].apply(lambda x: " ".join(x)) + " " +      # Genre names
    df['keywords'].apply(lambda x: " ".join(x)) + " " +    # Thematic keywords
    df['cast'].apply(join_names_from_cast) + " " +         # Top 5 actors
    df['director']                   # Director name
)
```

**Example Combined Features**:
```
"A thief who steals corporate secrets through dream-sharing technology 
is given the inverse task... Your mind is the scene of the crime. 
Science Fiction Action Thriller dream heist subconscious mind 
Leonardo DiCaprio Joseph Gordon-Levitt Ellen Page Christopher Nolan"
```

**TF-IDF Vectorization**:
- Converts text to numerical vectors
- **TF (Term Frequency)**: How often a word appears in this movie's description
- **IDF (Inverse Document Frequency)**: Downweights common words across all movies
- Result: Unique words like "heist" and "subconscious" get higher weights

---

## 🌐 The Django Web Application

### Architecture Overview

The website follows a **hybrid caching strategy** that minimizes API calls while ensuring data freshness:

```
┌─────────────────────────────────────────────────────────────────┐
│                      USER REQUEST                                │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Django View Layer                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  1. Check Local SQLite DB (Cache Hit?)                   │   │
│  │     - Movie model stores all static data                 │   │
│  │     - JSONFields for complex data (cast, genres, etc.)   │   │
│  └────────────────────────┬────────────────────────────────┘   │
│                           │                                      │
│           ┌───────────────┴───────────────┐                     │
│           │                               │                     │
│           ▼                               ▼                     │
│     CACHE HIT                       CACHE MISS                  │
│   ┌──────────────┐              ┌──────────────────┐            │
│   │ Return DB    │              │ Fetch from TMDB  │            │
│   │ data + fetch │              │ API & Save to DB │            │
│   │ fresh media  │              │ for next time    │            │
│   └──────────────┘              └──────────────────┘            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Optimized Data Fetching** (from `views.py`):

```python
def get_optimized_movie_display_data(movie_obj):
    """
    Helper function to get poster_path and vote_average from local DB.
    Only fetches from API if either field is missing.
    SAVES the fetched data to DB so next time it's faster!
    """
    poster_path = movie_obj.poster_path
    vote_average = movie_obj.vote_average
    updated = False
    
    # Only fetch from API if data is missing
    if not poster_path or vote_average is None:
        print(f"API call for movie {movie_obj.title} ({movie_obj.tmdb_id})")
        api_data = fetch_tmdb_data(f'movie/{movie_obj.tmdb_id}')
        if api_data:
            if not poster_path:
                poster_path = api_data.get('poster_path')
                movie_obj.poster_path = poster_path
                updated = True
            if vote_average is None:
                vote_average = api_data.get('vote_average')
                movie_obj.vote_average = vote_average
                updated = True
            if updated:
                movie_obj.save()  # Save to DB for next time!
    else:
        print(f"Using cached data for {movie_obj.title}")
    
    return poster_path, vote_average
```

### Search & Discovery System

The search system uses a **two-tier approach**: substring matching first, with fuzzy matching as a fallback.

#### How It Works

**Step 1: Substring Matching (Fast & Exact)**
```python
# User searches: "aveng"
qs = Movie.objects.filter(title__icontains="aveng").order_by('-popularity')

# Results (sorted by popularity):
# 1. Avengers: Endgame
# 2. Avengers: Infinity War
# 3. The Avengers
# 4. Avengers: Age of Ultron
# ...
```

**Step 2: Fuzzy Matching Fallback (Handles Typos)**
```python
from rapidfuzz import process as fuzzy_process

# User searches: "avengerz" (typo!)
# Substring match finds nothing...

# Fuzzy matching kicks in:
matches = fuzzy_process.extract("avengerz", titles_list, limit=5)

# Returns: [("Avengers: Endgame", 92, idx), ("The Avengers", 88, idx), ...]
# Score threshold: 60 (filters out poor matches)
```

#### Search Types

| Type | Query | How It Works |
|------|-------|--------------|
| **Title** | "inception" | `title__icontains` → fuzzy fallback |
| **Director** | "nolan" | `director__icontains` → fuzzy on all directors |
| **Actor** | "leonardo" | Iterates through `cast` JSONField, matches names |
| **Genre** | "thriller" | Iterates through `genres` JSONField |

#### Examples

**Title Search with Selection**:
```
User types: "dark knight"
→ Shows dropdown: "The Dark Knight", "The Dark Knight Rises", "Dark Knight"
→ User clicks "The Dark Knight"
→ Shows recommendations: "Batman Begins", "Inception", "The Prestige"...
```

**Fuzzy Matching Example**:
```
User types: "Quenten Tarentino" (misspelled)
→ Substring match fails
→ Fuzzy match finds "Quentin Tarantino" (score: 85)
→ Returns all Tarantino films sorted by popularity
```

### Personalized "For You" Recommendations

The "For You" feature generates personalized recommendations based on the user's liked movies:

```python
# recommender.py

def get_user_recommendations(user, n_per_movie: int = 5) -> List[int]:
    """
    Aggregates recommendations from all liked movies.
    """
    # 1. Get all movies the user has liked
    liked_ids = list(Like.objects.filter(user=user).values_list("movie_id", flat=True))
    
    if not liked_ids:
        return []  # No likes = no personalized recommendations
    
    # 2. Get top 5 similar movies for EACH liked movie
    all_recommendations = []
    seen_ids = set(liked_ids)  # Don't recommend already-liked movies
    
    for mid in liked_ids:
        similar_ids = get_similar_movies(mid, n=n_per_movie)
        for sim_id in similar_ids:
            if sim_id not in seen_ids:
                all_recommendations.append(sim_id)
                seen_ids.add(sim_id)
    
    # 3. Sort by popularity (most popular first)
    movies_qs = Movie.objects.filter(tmdb_id__in=all_recommendations).order_by('-popularity')
    
    return [m.tmdb_id for m in movies_qs]
```

**Example Flow**:
```
User has liked: "Inception", "The Dark Knight", "Interstellar"

For "Inception" (n=5):       → Shutter Island, The Matrix, Memento, Tenet, Source Code
For "The Dark Knight" (n=5): → Batman Begins, The Dark Knight Rises, Joker, V for Vendetta, Heat
For "Interstellar" (n=5):    → Gravity, The Martian, Arrival, Contact, 2001: A Space Odyssey

Combined & deduplicated, sorted by popularity:
→ The Matrix, Joker, Gravity, Shutter Island, Batman Begins, The Martian...
```

### User Interactions

The application supports three types of user interactions:

| Interaction | Description | Database Model |
|-------------|-------------|----------------|
| ❤️ **Like** | Toggle like/unlike on movies | `Like` model |
| 🔖 **Watchlist** | Save movies to watch later | `Watchlist` model |
| ⭐ **Review** | Rate (1-10) and review movies | `Review` model |

All interactions are stored in SQLite and used for personalization.

---

## 📈 Exploratory Data Analysis

After building the recommendation model, we performed comprehensive analysis to understand our data and answer business questions.

### SQL Analysis

We analyzed the cleaned dataset using MySQL to answer key questions:

#### Dataset Overview Queries

```sql
-- How many movies are in the dataset?
SELECT COUNT(title) FROM movies;
-- Result: 95,445 movies

-- What is the date range of our movies?
SELECT 
    MIN(release_date) as earliest,
    MAX(release_date) as latest
FROM movies;
-- Result: 1874 (earliest) to 2025 (latest)
```

#### Financial Analysis

```sql
-- Top 10 movies by profit
SELECT 
    title, 
    tagline,
    (revenue - budget) as profit
FROM movies
WHERE revenue != 0 AND budget != 0
ORDER BY profit DESC
LIMIT 10;

-- Result:
-- 1. Avatar                    - $2.5B profit
-- 2. Avengers: Endgame        - $2.3B profit
-- 3. Titanic                   - $2.0B profit
-- ...

-- Top 5 directors by total revenue
SELECT 
    director,
    SUM(revenue) as total_revenue
FROM movies
WHERE revenue != 0
GROUP BY director
ORDER BY total_revenue DESC
LIMIT 5;

-- Result: Steven Spielberg, James Cameron, Joe Russo, Peter Jackson, David Yates
```

#### Director Performance Analysis

```sql
-- Which directors consistently deliver above-average popularity?
SELECT 
    director,
    COUNT(*) as movie_count
FROM movies
WHERE popularity > (SELECT AVG(popularity) FROM movies)
GROUP BY director
HAVING director != 'unknown'
ORDER BY movie_count DESC;

-- Directors with highest ROI (revenue/budget ratio)
SELECT 
    director,
    ROUND(AVG(revenue)/AVG(budget), 2) as roi_ratio
FROM movies
WHERE revenue != 0 AND budget != 0
GROUP BY director
ORDER BY roi_ratio DESC;
```

#### Genre & Certification Insights

```sql
-- How many movies per certification?
SELECT 
    TRIM(certification) as rating,
    COUNT(*) as count
FROM movies
GROUP BY TRIM(certification)
ORDER BY count DESC;

-- Result:
-- N/A: 60,658 (mostly older films without ratings)
-- R: 12,170
-- NR: 10,584
-- PG-13: 5,180
-- PG: 4,613
-- G: 2,045

-- What percentage of each certification is over 120 minutes?
SELECT 
    TRIM(certification) AS rating,
    SUM(runtime > 120) * 100 / COUNT(*) AS percentage_over_120
FROM movies
WHERE TRIM(certification) != 'n/a'
GROUP BY TRIM(certification)
ORDER BY percentage_over_120 DESC;
```

#### Time-Based Analysis

```sql
-- Which year had the most movie releases?
SELECT 
    YEAR(release_date) as year,
    COUNT(*) as num_movies
FROM movies
WHERE release_date IS NOT NULL
GROUP BY YEAR(release_date)
ORDER BY num_movies DESC;

-- Average popularity score per decade
SELECT 
    FLOOR(YEAR(release_date)/10) * 10 as decade,
    ROUND(AVG(popularity), 3) as avg_popularity
FROM movies
WHERE release_date IS NOT NULL
GROUP BY decade
ORDER BY decade ASC;
```

#### Budget vs Revenue Correlation

```sql
-- Does higher budget lead to higher revenue?
SELECT 
    FLOOR(budget/10000000) * 10000000 as budget_group,
    ROUND(AVG(revenue)) as avg_revenue,
    COUNT(*) as num_movies
FROM movies
WHERE budget != 0 AND revenue != 0
GROUP BY budget_group
ORDER BY budget_group ASC;

-- Finding: Strong positive correlation - higher budgets generally 
-- correlate with higher revenues, but with diminishing returns above $200M
```

### Power BI Visualizations

We created an interactive Power BI dashboard to visualize our findings:

#### Dashboard Overview

| KPI Card | Value |
|----------|-------|
| **Count of Movies** | 95,445 |
| **Average Runtime** | 90.10 min |
| **Average Popularity** | 1.98 |
| **Sum of Revenue** | $769 Billion |

#### Key Visualizations

**1. Certification Distribution (Bar Chart)**
- Shows the breakdown of movie ratings
- N/A dominates (60,658) due to older films
- R-rated films are most common among rated movies (12,170)

**2. Top 10 Directors by Profitability (Horizontal Bar)**
| Director | Total Profit |
|----------|--------------|
| Steven Spielberg | $8.5B |
| James Cameron | $7.7B |
| Joe Russo | $5.4B |
| Peter Jackson | $5.2B |
| David Yates | $4.9B |
| Michael Bay | $4.8B |
| Christopher Nolan | $4.7B |
| Tim Burton | $3.4B |
| J.J. Abrams | $3.4B |
| Chris Columbus | $3.3B |

**3. Top 10 Movies by Profit (Horizontal Bar)**
| Movie | Profit |
|-------|--------|
| Avatar | $3B |
| Avengers: Endgame | $2B |
| Ne Zha 2 | $2B |
| Titanic | $2B |
| Avatar: The Way of Water | $2B |
| Star Wars: The Force Awakens | $2B |
| Avengers: Infinity War | $2B |
| Spider-Man: No Way Home | $2B |
| Jurassic World | $2B |
| Inside Out 2 | $1B |

**4. Movies & Popularity Over Time (Dual-Axis Line Chart)**
- X-axis: Year (1880-2020)
- Left Y-axis: Average Popularity (line, green)
- Right Y-axis: Count of Movies (line, blue)
- **Insight**: Both movie count and popularity have exponentially increased since 2000, with a notable spike around 2010-2020

**5. Top 10 Genres (Horizontal Bar)**
| Genre | Movie Count |
|-------|-------------|
| Drama | 41K |
| Comedy | 28K |
| Thriller | 15K |
| Romance | 14K |
| Documentary | 12K |
| Action | 11K |
| Horror | 11K |
| Crime | 10K |
| Adventure | 7K |
| Family | 6K |

**6. Top 10 Actors by Total Profit (Horizontal Bar)**
| Actor | Total Profit |
|-------|--------------|
| Stan Lee | $23B |
| Samuel L. Jackson | $21B |
| Frank Welker | $17B |
| Jess Harnell | $15B |
| John Ratzenberger | $14B |
| Alan Tudyk | $14B |
| Scarlett Johansson | $14B |
| Laraine Newman | $13B |
| Vin Diesel | $13B |
| Warwick Davis | $13B |

#### Power BI Data Model

To properly analyze genres and cast (stored as lists), we created **two additional tables** in Power BI:

**1. Genres_Table**
```
┌─────────────┬──────────────┐
│ genres      │ movie_id     │
├─────────────┼──────────────┤
│ Action      │ 12345        │
│ Adventure   │ 12345        │
│ Sci-Fi      │ 12345        │
│ Drama       │ 12346        │
│ ...         │ ...          │
└─────────────┴──────────────┘
```
Each genre is split into its own row, linked to the movie.

**2. Cast_Table**
```
┌─────────────────┬──────────────┐
│ cast.name       │ movie_id     │
├─────────────────┼──────────────┤
│ Leonardo DiCaprio│ 12345       │
│ Joseph Gordon-Levitt│ 12345   │
│ Ellen Page      │ 12345        │
│ ...             │ ...          │
└─────────────────┴──────────────┘
```
Each actor is split into their own row, enabling accurate counts.

**Relationships**:
- `movies_db_movies.movie_id` → `Cast_Table.movie_id` (1:Many)
- `movies_db_movies.movie_id` → `Genres_Table.movie_id` (1:Many)

---

## 🗄 Database Schema

### Django Models

```python
class Movie(models.Model):
    # Primary Key
    tmdb_id = models.IntegerField(unique=True, primary_key=True)
    
    # Core Data
    title = models.CharField(max_length=255)
    overview = models.TextField()
    tagline = models.CharField(max_length=255, null=True, blank=True)
    
    # Numeric Data
    release_date = models.DateField(null=True, blank=True)
    runtime = models.IntegerField(null=True, blank=True)
    popularity = models.FloatField(null=True, blank=True)
    budget = models.BigIntegerField(null=True, blank=True)
    revenue = models.BigIntegerField(null=True, blank=True)
    vote_average = models.FloatField(null=True, blank=True)
    
    # Image/Media
    poster_path = models.CharField(max_length=255, null=True, blank=True)
    
    # Complex JSON Data
    genres = models.JSONField(default=list)      # [{"id": 28, "name": "Action"}, ...]
    keywords = models.JSONField(default=list)    # [{"id": 123, "name": "heist"}, ...]
    cast = models.JSONField(default=list)        # [{"name": "Actor", "character": "Role", "profile_path": "/..."}, ...]
    external_ids = models.JSONField(default=dict)  # {"imdb_id": "tt1234567", ...}
    
    # Text Fields
    director = models.CharField(max_length=255, null=True, blank=True)
    certification = models.CharField(max_length=10, null=True, blank=True)
    original_language = models.CharField(max_length=10, null=True, blank=True)
```

### User Interaction Models

```python
class Like(BaseInteraction):
    """Records when a user likes a movie"""
    pass  # Inherits user, movie_id, created_at

class Watchlist(BaseInteraction):
    """Records when a user saves a movie to watch later"""
    pass

class Review(BaseInteraction):
    """User review with rating and optional text"""
    rating = models.IntegerField(validators=[MinValueValidator(1), MaxValueValidator(10)])
    text = models.TextField(blank=True, null=True)
```

---

## 📁 Project Structure

```
recommendation system/
├── 📓 Jupyter Notebooks
│   ├── movies data set.ipynb      # Data collection from TMDB API
│   ├── data merge .ipynb          # Combining multiple data sources
│   ├── db_clean.ipynb             # Data cleaning & preprocessing
│   ├── recommendation system.ipynb # ML model development
│   ├── movies meta data .ipynb    # Additional metadata collection
│   └── new movies.ipynb           # Adding 2024-2025 movies
│
├── 📊 Data Files
│   ├── movies_DB_cleaned.csv      # Final cleaned dataset (95,445 movies)
│   ├── recommendation system.pkl  # Trained NearestNeighbors model
│   └── tfidf_vectorizer.pkl       # Fitted TF-IDF vectorizer
│
├── 📈 Analytics
│   └── movies EDA.sql             # SQL analysis queries
│
└── 🌐 website/                    # Django Application
    ├── manage.py
    ├── db.sqlite3                 # Production database
    │
    ├── MovieSite/                 # Django project settings
    │   ├── settings.py
    │   ├── urls.py
    │   └── wsgi.py
    │
    ├── movies/                    # Main application
    │   ├── models.py              # Database models
    │   ├── views.py               # View functions (~990 lines)
    │   ├── recommender.py         # ML recommendation logic
    │   ├── utils.py               # TMDB API utilities
    │   ├── forms.py               # Django forms
    │   ├── urls.py                # URL routing
    │   │
    │   ├── management/commands/   # Custom management commands
    │   │   ├── import_movies.py   # Import CSV to database
    │   │   └── fix_external_ids.py
    │   │
    │   └── templatetags/
    │       └── custom_filters.py  # Custom template filters
    │
    └── templates/                 # HTML Templates
        ├── base.html              # Base template with navigation
        ├── movies/
        │   ├── home.html          # Homepage with filters
        │   ├── detail.html        # Movie detail page
        │   ├── search.html        # Search results
        │   ├── watchlist.html     # User's watchlist
        │   └── cast_detail.html   # Full cast view
        │
        └── registration/
            ├── login.html
            └── register.html
```

---