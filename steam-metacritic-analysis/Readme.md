
# 🎮 Steam & Metacritic Game Analysis

## 📌 Project Overview

This project combines data from Metacritic and Steam to analyze top-rated PC games. It uses web scraping, API integration, SQL cleaning, fuzzy matching, and Power BI visualizations to uncover trends in game ratings, pricing, popularity, and developer/publisher dominance.

## 🧠 What You’ll Learn

- Scraping structured data from websites
- Extracting and processing API data
- Cleaning and transforming datasets using SQL
- Matching inconsistent text entries using fuzzy logic
- Performing EDA with SQL queries
- Building interactive dashboards with Power BI

## 🛠️ Tools & Technologies

| Tool          | Purpose                            |
|---------------|-------------------------------------|
| Python        | Web scraping, API calls, fuzzy matching |
| BeautifulSoup | HTML parsing for Metacritic         |
| SteamSpy API  | Game metadata from Steam            |
| RapidFuzz     | Fuzzy string matching               |
| SQL (MySQL)   | Data cleaning and EDA               |
| Power BI      | Data visualization                  |
| Pandas        | Data manipulation                   |

## 📁 Project Structure

## 🔍 Step-by-Step Workflow

### 1. Web Scraping Metacritic
- Scraped top PC games using BeautifulSoup
- Extracted title, release date, rating, description, score

### 2. SteamSpy API Integration
- Queried SteamSpy API for game metadata
- Extracted developer, publisher, price, playtime, reviews, ownership

### 3. Data Cleaning with SQL
- Converted dates and scores
- Handled nulls and standardized columns
- Transformed price from cents to USD
- Derived estimated owners

### 4. Exploratory Data Analysis (EDA)
- Rating distributions, release trends, score categories
- Top developers/publishers, review sentiment, playtime metrics
- Price tier categorization

### 5. Fuzzy Matching with RapidFuzz
- Matched game titles across datasets
- Filtered by similarity score for high-confidence joins

### 6. Unified Dataset Creation
- Joined matched titles with cleaned data
- Final table includes game name, score, rating, price, developer, publisher, reviews, playtime, ownership

### 7. Visualization with Power BI
- Dashboards include:
  - Top-rated games
  - Genre and rating distributions
  - Developer/publisher dominance
  - Release trends
  - Free-to-play analysis
  - Price tier breakdowns

## 📸 Dashboard Highlights

| Visualization | Description |
|---------------|-------------|
| 🎯 Top 10 Highest Rated Games | Table of elite titles by score |
| 🧩 Genre Distribution | Pie chart of game genres |
| 🔞 Rating Breakdown | ESRB rating distribution |
| 🏗️ Top Developers & Publishers | Bar charts of most prolific creators |
| 📅 Release Trends | Line graph of game launches by year |
| 💰 Price Tier Analysis | Pie chart of games by price category |
| 🆓 Free Games | Table of most-owned free-to-play titles |

## 📦 How to Run Locally

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/my-projects.git
   cd my-projects/steam-metacritic-analysis
