-- Data cleaning

select *
from spotify_api;

-- check for null values

select *
from spotify_api
where track_name is null or artist_name is null or album_name is null;

select *
from spotify_api
where album_name like '%1989%';

SELECT distinct
    t1.track_name,
    t1.album_name,
	t1.artist_name ,
    t1.artist_popularity,
    t1.artist_followers,
    t2.artist_genres,
    t2.artist_name AS populated_artist_name,
    t2.artist_popularity,
    t2.artist_followers
FROM spotify_api AS t1
JOIN spotify_api AS t2
    ON t1.album_name = t2.album_name
WHERE t1.artist_name IS NULL
  AND t2.artist_name IS NOT NULl
  and t1.album_name like "1989%";

update spotify_api as t1
JOIN spotify_api AS t2
 ON t1.album_name = t2.album_name
set t1.artist_name = t2.artist_name,
t1.artist_popularity =t2.artist_popularity,
t1.artist_followers = t2.artist_followers,
 t1.artist_genres= t2.artist_genres
WHERE t1.artist_name IS NULL
  AND t2.artist_name IS NOT NULl
  and t1.album_name like "1989%";
 

-- drop rows where track name is null

delete
from spotify_api
where track_name is null ;

update spotify_api
set artist_name = coalesce(artist_name,'N/A'),
artist_popularity = coalesce(artist_popularity, 0),
artist_followers = coalesce(artist_followers,0),
artist_genres = coalesce(artist_genres,'[]'); -- for now till we deal with it later 

-- standardize some columns 

update spotify_api
set track_name = TRIM(track_name),
artist_name = TRIM(artist_name),
album_name = trim(album_name),
album_type = trim(album_type),
explicit = trim(explicit),
explicit = upper(explicit);

select *
from spotify_api;

-- fix the release date column

-- we got some dates where we have the year only we will estimate the day and month to be mid year so we can covert all dates safely  
select album_release_date, CONCAT(album_release_date, '-06-30')
from spotify_api
where length(album_release_date) = 4;



update spotify_api
set album_release_date = CONCAT(album_release_date, '-06-30')
where length(album_release_date) = 4;

select album_release_date , str_to_date(album_release_date,'%Y-%m-%e')
from spotify_api;

update spotify_api
set album_release_date = str_to_date(album_release_date,'%Y-%m-%e');

-- we got some dates with year and month without the day 

select album_release_date, CONCAT(album_release_date, '-01')
from spotify_api
where length(album_release_date) = 7;

update spotify_api
set album_release_date = CONCAT(album_release_date, '-01')
where length(album_release_date) = 7;


update spotify_api
set album_release_date = str_to_date(album_release_date,'%Y-%m-%e');

-- covert to date data type
alter table spotify_api
modify column album_release_date date;

select album_release_date
from spotify_api;

-- make a new column for the duration in mins 

select track_duration_ms, truncate((track_duration_ms/60000),2) as min
from spotify_api;

alter table spotify_api
add column track_duration_min double ;

update spotify_api
set track_duration_min = truncate((track_duration_ms/60000),2);

select *
from spotify_api;

-- fix the genres column

select artist_genres, REGEXP_REPLACE(artist_genres,"['\\[\\]]","")
from spotify_api;

update spotify_api
set artist_genres = REGEXP_REPLACE(artist_genres,"['\\[\\]]","");

select *
from spotify_api
where artist_genres ="" ;

-- some artists generes were not available from the spotify api so its now empty we can populate it for some of the more famous artists 

update spotify_api
set artist_genres ="country, pop, indie, folk"
where artist_genres ="" and artist_name = "Taylor Swift";

update spotify_api
set artist_genres ="pop rock, alternative pop, pop punk"
where artist_genres ="" and artist_name = "olivia rodrigo";

update spotify_api
set artist_genres ="alternative pop, electropop, dark pop"
where artist_genres ="" and artist_name = "billie eilish";

update spotify_api
set artist_genres ="pop"
where artist_genres ="" and artist_name = "tate mcrae";

update spotify_api
set artist_genres ="country pop, rock, hip hop, experimental"
where artist_genres ="" and artist_name = "miley cyrus";

update spotify_api
set artist_genres ="pop, contemporary r&b"
where artist_genres ="" and artist_name = "justin bieber";

update spotify_api
set artist_genres ="pop rock, soft rock, synthpop"
where artist_genres ="" and artist_name = "harry styles";

update spotify_api
set artist_genres ="alternative pop, indie pop"
where artist_genres ="" and artist_name = "lana del rey";

update spotify_api
set artist_genres ="r&b pop, alternative r&b, hip hop, synthpop"
where artist_genres ="" and artist_name = "the weeknd";

update spotify_api
set artist_genres ="pop, r&b"
where artist_genres ="" and artist_name = "rihanna";

update spotify_api
set artist_genres ="pop, hip hop, country"
where artist_genres ="" and artist_name = "post malone";



update spotify_api
set artist_genres = 'N/A'
where artist_genres ="";

-- we populted as much as we could 

select *
from spotify_api;

-- now we remove duplicates if exists and columns we dont need 

alter table spotify_api
drop column track_duration_ms;


select *
from
(select *, row_number() over ( partition by track_name , artist_name , album_release_date, album_name 
order by album_release_date asc) as row_num
from spotify_api) as t1
where row_num> 1;

create table spotify_api_clean as
select*,row_number() over ( partition by track_name , artist_name , album_release_date, album_name 
order by album_release_date asc) as row_num
from spotify_api;

select *
from spotify_api_clean
where row_num> 1;

delete
from spotify_api_clean
where row_num> 1;

alter table spotify_api_clean
drop column row_num ;

-- now are data is ready for EDA
select*
from spotify_api_clean
order by album_release_date desc;
