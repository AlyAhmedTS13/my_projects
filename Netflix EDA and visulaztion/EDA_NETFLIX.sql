-- EDA PROJECT ON NETFLIX DATA SET AFTER DOING SOME CLEANING 


-- QUESTION 1 Count the number of Movies vs TV Shows:

select count(type) as tv_show_count,(select count(type) as movie_count
from netflix_copy2
where type = 'movie') as movie_count
from netflix_copy2
where type = 'tv show';

-- another solution:

select type, count(*) as movie_Vs_tv_show_count
from netflix_copy2
group by 1;
-- QUESTION 2 Find the most common rating for movies and TV shows:

with t1 as (select rating, type , count(*) as count
from netflix_copy2
group by 1,2
order by 3 desc),
  t2 as
(select rating,type,count,row_number() over(partition by type) as row_num
from t1 )
select rating, type
from t2 
where row_num =1;




-- QUESTION 3 List all movies released in a specific year (e.g., 2020):

select title 
from netflix_copy2
where release_year = 2020 and type = "movie";

-- QUESTION 4 Find the top 5 countries with the most content on Netflix

WITH RECURSIVE split_countries AS (
    -- Base case: Start with the full column and extract the first country
    SELECT 
        TRIM(SUBSTRING_INDEX(country, ',', 1)) AS country,
        TRIM(SUBSTR(country, LENGTH(SUBSTRING_INDEX(country, ',', 1)) + 2)) AS remaining
    FROM netflix_copy2
    
    UNION ALL
    
  
    SELECT 
        TRIM(SUBSTRING_INDEX(remaining, ',', 1)) AS country,
        TRIM(SUBSTR(remaining, LENGTH(SUBSTRING_INDEX(remaining, ',', 1)) + 2)) AS remaining
    FROM split_countries
    WHERE remaining LIKE '%,%'
)

SELECT 
    country, 
    COUNT(*) AS country_count
FROM split_countries
GROUP BY country
HAVING country IS NOT NULL
ORDER BY country_count DESC
LIMIT 5;

-- QUESTION 5 Identify the longest movie:

with t1 as (SELECT 
    CAST(REPLACE(duration, 'min', '') AS SIGNED) AS duration_int,title
FROM 
    netflix_copy2
where type = "movie")
select title
from t1
order by duration_int desc
limit 1;

-- QUESTION 6 Find content added in the last 5 years:

SELECT *
from netflix_copy2
where Date_added_converted >= curdate() -interval 5 year
order by Date_added_converted ;

-- QUESTION 7 Find all the movies/TV shows by director 'Quentin Tarantino'!:

select *
from netflix_copy2
where director = 'Quentin Tarantino';

-- QUESTION 8 List all TV shows with more than 5 seasons:

  with t1 as (select cast(replace(duration,'Season',"") as signed) as duration_int,type,title
from netflix_copy2
where type = "tv show")
select title
from t1
where duration_int >= 5
order by duration_int;

-- QUESTION 9 Count the number of content items in each genre:
with RECURSIVE t1 as (
select
	trim(substring_index(listed_in,",",1)) as genre,
    trim(substr(listed_in,length(substring_index(listed_in,",",1))+2)) as remaining
from netflix_copy2

union all

select 
	trim(substring_index(remaining,",",1)) as genre,
    trim(substr(remaining,length(substring_index(remaining,",",1))+2)) as remaining
from t1
where remaining like "%,%"
)
select  genre , count(*) as count_of_content
from t1
group by genre
having genre is not null
order by 2 desc;

-- QUESTION 10 Find the average number of content released in USA on netflix, return top 5 years with highest avg content release!

 with t1 as (select*
 from netflix_copy2
 where country = 'united states')
select year(Date_added_converted) as year_released, count(show_id) as count_of_content 
from t1
group by year_released
order by count_of_content  desc
limit 5;

-- QUESTION 11 List all movies that are documentaries:


select title
from netflix_copy2
where listed_in like "%documentaries%";

-- QUESTION 12  Find all content without a director :

select title
from netflix_copy2
where director is null;

-- QUESTION 13 Find how many movies actor 'Al Pacino' appeared in last 10 years!

select *
from netflix_copy2
where casts like "%Al Pacino%" and Date_added_converted>= current_date() - interval 10 year;

-- QUESTION 14 Find the top 10 actors who have appeared in the highest number of movies produced in USA:
with recursive t1 as (
select trim(substring_index(casts,',',1)) as actor,
	   trim(substr(casts,length(substring_index(casts,',',1))+2)) as remaining,
	   country
from 
	netflix_copy2
    
union all

select
trim(substring_index(remaining,',',1)) as actor,
trim(substr(remaining,length(substring_index(remaining,',',1))+2)) as remaining,country
from t1 
where remaining like "%,%"

)
select actor, count(*) as num_movies,country
from t1
group by actor,country
having actor is not null and country  like '%united states%'
order by 2 desc
limit 10;

-- 	QUESTION 15 Categorize the content based on the presence of the keywords 'kill' and 'violence' in 
-- the description field. Label content containing these keywords as 'Bad' and all other 
-- content as 'Good'. Count how many items fall into each category.

select *
from netflix_copy2;

 with t1 as (select 
case
when `description` like "%violence%" or `description` like '%kill%' then 'Bad'
else 'Good'
end as  label
from netflix_copy2)
select label, count(*) as num_content
from t1 
group by 1


















