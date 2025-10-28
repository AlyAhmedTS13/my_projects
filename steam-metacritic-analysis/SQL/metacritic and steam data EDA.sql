-- EDA for both our data sets
-- we will start with metacritic data set
select *
from metacritic_toppc_games;

--  Retrieve the top 10 highest-rated PC games
select *
from metacritic_toppc_games
order by score desc
limit 10;

-- Count how many games belong to each rating category (e.g. E, M, T) 
SELECT rating, COUNT(*) AS count
FROM metacritic_toppc_games
GROUP BY rating
ORDER BY count DESC;

 -- Count how many games were released per year
SELECT YEAR(release_date) AS year, COUNT(*) AS count
FROM metacritic_toppc_games
GROUP BY YEAR(release_date)
ORDER BY count DESC;

-- Distribution of games by score
select score, count(*) as count
from metacritic_toppc_games
group by score
order by count desc;

-- Categorize each game based on its score (High / Mid / Low rated)
select *, case
	when score >= 80 then 'High rated game'
    when score >= 60 and score <80 then 'Mid rated game'
    when score < 60 then 'Low rated game'
end as category
from metacritic_toppc_games;

-- Count how many games fall into each category (High, Mid, Low)
select category, count(*) as count
from (
	select *, case
		when score >= 80 then 'High rated game'
		when score < 80 and score >= 60 then 'Mid rated game'
		when score < 60 then 'Low rated game'
	end as category
	from metacritic_toppc_games
) as t1
group by category;


-- game of the year choices for metacritics for some years 
select *
from metacritic_toppc_games
where Description like "[metacritic's%";


-- now the steam data set
select *
from steam_popular_games;


-- Get top 10 developers with the most games
select developer, count(*) as count
from steam_popular_games
where developer != 'N/A'
group by developer
order by count desc
limit 10;
 
 --  Get top 10 publishers with the most games
select publisher, count(*) as count
from steam_popular_games
where publisher != 'N/A'
group by publisher
order by count desc
limit 10;

-- Calculate and rank games by percentage of positive reviews
select *, case
	when round(positive / (positive + negative) * 100, 0) is not null then round(positive / (positive + negative) * 100, 0)
	when round(positive / (positive + negative) * 100, 0) is null then 0 # to deal with games with 0 reviews as we cant 0/0
end as percentage_of_positive_reviews
from steam_popular_games
order by percentage_of_positive_reviews desc;

--  game with the most playtime 
select name, price_usd, average_playtime_total, publisher, developer, estimated_owners
from steam_popular_games
order by average_playtime_total desc
limit 1;

-- Get top 10 most-owned games
select name, price_usd, average_playtime_total, publisher, developer, estimated_owners
from steam_popular_games
order by estimated_owners desc
limit 10;

-- get free games on steam 
select name, price_usd, average_playtime_total, publisher, developer, estimated_owners, peak_current_players_yesterday
from steam_popular_games
where price_usd = (
	select min(price_usd)
	from steam_popular_games
)
order by name;

-- most expensive game on steam 
select name, price_usd, average_playtime_total, publisher, developer, estimated_owners
from steam_popular_games
order by price_usd desc
limit 1;

 -- Categorize games by price range (Free, Budget, Standard, Premium, Deluxe)
select name, average_playtime_total, publisher, developer, estimated_owners, peak_current_players_yesterday, price_usd,
case
	when price_usd = 0 then 'Free to play'
	when price_usd > 0 and price_usd <= 5 then 'Very cheap'
	when price_usd > 5 and price_usd <= 20 then 'Budget'
	when price_usd > 20 and price_usd <= 40 then 'Standard'
	when price_usd > 40 and price_usd <= 70 then 'Premium'
	when price_usd > 70 then 'Deluxe'
end as price_category
from steam_popular_games
order by estimated_owners desc;

/* we will use the table we made with matching names from metacritic data and steam data and take important columns from both data sets
we have the metacritic game name and its steam or closet smiliar name plus the price and release data etc */

create table meta_steam_names as 
SELECT
  m.metacritic_name,
  m.steam_name,
  m.similarity_score,
  s.publisher,
  s.developer,
  s.price_USD,
  s.initialprice_USD,
  s.`discount_%`,
  s.estimated_owners,
  s.average_playTime_total,
  s.peak_Current_Players_yesterday,
  s.positive,
  s.negative,
  mc.release_Date,
  mc.Rating,
  mc.score
FROM joined_tables m
JOIN steam_popular_games s ON m.steam_name = s.Name
JOIN metacritic_toppc_games mc ON m.metacritic_name = mc.Name;

-- we will remove the duplicates from column
create table meta_steam_names_copy
select *
from (
select*,row_number() over ( partition by metacritic_name, publisher, developer, release_Date order by
similarity_score desc ) as row_num
from meta_steam_names) as t1;


delete
from meta_steam_names_copy
where row_num > 1;

select *
from meta_steam_names_copy;

alter table meta_steam_names_copy
drop column row_num;


-- final table
select *
from meta_steam_names_copy
order by similarity_score desc ;



    
