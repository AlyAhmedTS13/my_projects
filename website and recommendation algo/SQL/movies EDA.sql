select *
from movies
limit 10 ;

-- How many movies are in the dataset in total?
select count(title)
from movies;

-- What is the earliest and latest release date recorded?
select max(release_date) as latest_date,min(release_date) as earliest_date
from movies;

select title,release_date
from movies 
where release_date =  (select min(release_date)
from movies);

select title,release_date
from movies 
where release_date =  (select max(release_date)
from movies);

-- Which movies has the longest and shortest runtime?

select title , runtime
from movies 
where runtime =  (select max(runtime)
from movies);

select title , runtime,movie_id
from movies 
where runtime =  (select min(runtime)
from movies);

-- How many movies have a budget of zero?

select count(*) as num_movies
from movies
where budget = 0;

-- What is the average revenue across all movies?

select round(avg(revenue),0) as average_revenue
from movies
where revenue !=0;

-- Which directors appear most frequently in the dataset?

select director , count(*) as num_of_appernces
from movies
group by director
having director != 'unknown'
order by num_of_appernces desc;


-- How many movies are certified "PG" vs "PG-13" vs "R"?

select trim(certification) as certification , count(*) as count
from movies
group by trim(certification) 
order by count desc;

-- What is the average runtime of movies per certification category?
select round(avg(runtime),1) as avg_runtime , trim(certification) as rating
from movies
group by trim(certification)
having rating != 'n/a'
order by avg_runtime desc;

-- Which year had the highest number of movies released?

select year(release_date) as `year`, count(*) as num_of_movies_released
from movies
group by year(release_date)
having `year` is not null
order by num_of_movies_released desc;

-- What is the average budget and revenue per director?

select director ,round(avg(budget),0) as average_budget,
round(avg(revenue),0) as average_revenue 
from movies
where revenue != 0 and budget != 0
group by director
having director != 'unknown'
order by director;

-- Which movies had revenue greater than 5× their budget?

select title , budget, revenue
from movies
where revenue > 5 * budget and budget !=0 and revenue != 0
order by title;

-- What is the average popularity score per decade?

select floor(year(release_date)/10) *10 as decade, 
round(avg(popularity),3) as avg_popularity
from movies
where release_date is not null
group by decade
order by decade asc;

-- Which directors have directed more than 3 movies in the dataset?

select director , count(*) as num_of_appernces
from movies
group by director
having director != 'unknown' and num_of_appernces > 3
order by num_of_appernces desc;

-- What is the top 10 movies by profit, and what are their taglines?

select title , tagline ,release_date, (revenue-budget) as profit
from movies
where revenue !=0 and budget != 0
order by profit desc
limit 10;

-- Which certification category has the highest average popularity?

select trim(certification) as rating,round(avg(popularity),2) as avg_popularity
from movies
group by trim(certification)
having rating != 'n/a'
order by avg_popularity desc;

-- Which movies had both high popularity (>50) and low revenue (<10M)?

select title ,popularity, revenue
from movies
where popularity > 50 and revenue < 1000000 and revenue != 0
order by popularity desc; 

-- For each director, what is the ratio of average revenue to average budget?

select director ,round(avg(revenue)/avg(budget),2) as ratio
from movies
where revenue !=0 and budget !=0
group by director
order by ratio desc;

-- Which year had the highest total revenue across all movies released that year?

select year(release_date) as year , sum(revenue) as total_revenue
from movies
where revenue !=0
group by year(release_date)
order by total_revenue desc;

-- Find the top 5 directors ranked by total revenue of their movies.

select director , sum(revenue) as total_revenue
from movies
where revenue !=0
group by director
order by total_revenue desc
limit 5;

-- Which movies had a budget above the overall average but revenue below the overall average?

select title , budget, revenue
from movies
where budget > (select avg(budget) 
from movies
where budget !=0 )
and revenue < (select avg(revenue) 
from movies
where revenue !=0)
and revenue !=0 and budget !=0
order by title;

-- For each certification, what percentage of movies had a runtime longer than 120 minutes?

select TRIM(certification) AS certifications,
    SUM(runtime > 120) * 100 / COUNT(*) AS percentage_over_120
from movies
where TRIM(certification) != 'n/a'
group by TRIM(certification)
order by percentage_over_120 desc;

-- Find the correlation trend: does higher budget generally lead to higher revenue 
-- (group movies into budget ranges and compare average revenue)?

select floor(budget/10000000)*10000000 as budget_group,
round(avg(revenue)) as avg_revenue , count(*) as num_of_movies
from movies
where budget !=0 and revenue != 0
group by budget_group
order by budget_group asc;


-- Which directors consistently deliver movies with above average popularity?
select director , count(*) as count
from movies
where popularity > ( select round(avg(popularity),1)
from movies)
group by director
having director != "unknown"
order by count desc;

-- How does certification distribution change across decades
-- (e.g., more PG13 in 2000s vs PG in 1990s)?

select floor(year(release_date)/10) * 10 as decade,
certification , count(*) as count
from movies
where release_date is not null and certification != "n/a"
group by certification ,decade
order by decade asc , count desc









