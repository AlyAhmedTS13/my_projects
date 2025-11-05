-- EDA

select *
from spotify_api_clean
where artist_name like "%nirvana%";


-- count number of explicit vs inexplicit tracks 

select explicit, count(*) as count
from spotify_api_clean
group by explicit
order by count desc ;

-- top 10 artists in the world by number of followers

select distinct  artist_name, max(artist_followers) as artist_followers
from spotify_api_clean
group by  artist_name
order by  artist_followers desc
limit 10;

-- Count of Album Types

select album_type, count(*) as count_of_type
from spotify_api_clean
group by album_type
order by count_of_type desc;

-- count number of tracks released each year
select year(album_release_date) as year , count(*)  as count_of_tracks
from spotify_api_clean
group by year(album_release_date)
order by year desc ;

-- longest and shortest song 
(select track_duration_min as `longest/shortest_track_inMINs`, track_name, artist_name
from spotify_api_clean
order by track_duration_min desc
limit 1)
union 
(select track_duration_min, track_name, artist_name
from spotify_api_clean
order by track_duration_min asc
limit 1);

-- count of number of artists we have
select count(distinct artist_name) as count_of_artist
from spotify_api_clean;

select distinct artist_name
from spotify_api_clean
order by artist_name ;

-- top most popular tracks ( 90 and above ) 
-- does artist popularity influences track success?

select distinct track_name, track_popularity , artist_name, artist_popularity
from spotify_api_clean
where track_popularity >= 90
order by track_popularity desc , artist_popularity desc;

-- Who Dominates Pop?

select artist_name , count(*) as count 
from spotify_api_clean
where artist_genres like '%pop%'
group by artist_name
order by count desc
limit 10;

-- most popular genres ( we will do it using pandas in python much easier )

