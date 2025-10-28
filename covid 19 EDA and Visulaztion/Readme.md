🌍 Global COVID-19 Analysis & Visualization
📌 Project Overview
This project analyzes global COVID-19 trends using SQL and Power BI, based on over 85,000 rows of case data and 85,000+ rows of vaccination records, recorded up to May 2021. It explores infection rates, death counts, vaccination progress, and country-level comparisons. The data is cleaned, transformed, and modeled to support interactive dashboards and geographic insights.

🧠 What You’ll Learn
How to work with large-scale datasets (170K+ rows combined)

How to clean and transform time-series health data using SQL

How to build scalable queries with window functions and views

How to model relationships for geographic filtering in Power BI

How to visualize global health trends with interactive dashboards

🛠️ Tools & Technologies
Tool	Purpose
SQL (MySQL)	Data cleaning, transformation, EDA
Power BI	Interactive dashboards and maps
Window Functions	Cumulative vaccination tracking
Views & Temp Tables	Reusable queries for visuals

🔍 SQL Workflow Summary
1.🧼 Data Cleaning
Converted date columns to proper DATE format

Renamed and replaced columns for clarity

Removed nulls and standardized structure

2.📊 Exploratory Analysis
Total cases vs. deaths → fatality rate

Total cases vs. population → infection rate

Highest infection % by country

Highest death count by country and continent

Global daily totals and death percentages

3.💉 Vaccination Analysis
Joined covid_deaths and covid_vacc by location and date

Used SUM() OVER() to calculate cumulative vaccinations

Compared total vaccinations vs. population

Created reusable views and temp tables for Power BI

🌍 Power BI Highlights
Visualization	Description
🗺️ Global Map	Hover to filter by country using DimLocation (custom-built)
📈 Cases vs. Vaccinations	Line chart over time
📊 Deaths by Country	Bar chart of top 10
🌐 Deaths by Continent	Aggregated totals
📅 Date Filter	Dynamic range selector
📦 Total Metrics	Cards showing total cases, deaths, and death percentage
💡 Note: The DimLocation table was manually created to enable dynamic filtering by country on the map — allowing users to hover over a country and see its specific metrics.

📦 How to Run Locally
Clone the repository:

bash
git clone https://github.com/yourusername/my-projects.git
cd my-projects/covid-global-analysis
Load SQL files into MySQL:

sql/cleaning.sql

sql/eda.sql

Open Power BI and import cleaned CSVs and views