-- DATA cleaning of 2023 top artists and songs charts
select *
from `spotify-2023`
;
-- #1 step remove duplicates if present
# first we need to make a copy of our dataset

create table  `spotify-2023_copy`
like `spotify-2023`;

select *
from `spotify-2023_copy`;

insert `spotify-2023_copy`
select *
from `spotify-2023`;

select *
from `spotify-2023_copy`; # our backup to work with 

#1 step

with dup as(
select *,row_number() over(partition by track_name,`artist(s)_name`,artist_count,released_year,released_month,
released_day,in_spotify_playlists,in_spotify_charts,streams,in_apple_playlists,in_apple_charts,in_deezer_playlists,in_deezer_charts,
in_shazam_charts,bpm,`key`,`mode`,`danceability_%`,`valence_%`,`energy_%`,`acousticness_%`,
`instrumentalness_%`,`liveness_%`,`speechiness_%`) as row_num
from `spotify-2023_copy`
)
select *
from dup
where row_num>1;

# no duplictes where found as entire rows have the same data entries 

with dup2 as(
select *,row_number() over(partition by track_name,`artist(s)_name`) as row_num
from `spotify-2023_copy`
)
select *
from dup2
where row_num>1;

select *
from `spotify-2023_copy`
where track_name="take my breath";

# we found duplictes where columns have the same track and atrist name 

CREATE TABLE `spotify-2023_copy2` (
  `track_name` text,
  `artist(s)_name` text,
  `artist_count` int DEFAULT NULL,
  `released_year` int DEFAULT NULL,
  `released_month` int DEFAULT NULL,
  `released_day` int DEFAULT NULL,
  `in_spotify_playlists` int DEFAULT NULL,
  `in_spotify_charts` int DEFAULT NULL,
  `streams` int DEFAULT NULL,
  `in_apple_playlists` int DEFAULT NULL,
  `in_apple_charts` int DEFAULT NULL,
  `in_deezer_playlists` int DEFAULT NULL,
  `in_deezer_charts` int DEFAULT NULL,
  `in_shazam_charts` int DEFAULT NULL,
  `bpm` int DEFAULT NULL,
  `key` text,
  `mode` text,
  `danceability_%` int DEFAULT NULL,
  `valence_%` int DEFAULT NULL,
  `energy_%` int DEFAULT NULL,
  `acousticness_%` int DEFAULT NULL,
  `instrumentalness_%` int DEFAULT NULL,
  `liveness_%` int DEFAULT NULL,
  `speechiness_%` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from `spotify-2023_copy2`;


insert  `spotify-2023_copy2`
select *,row_number() over(partition by track_name,`artist(s)_name`) as row_num
from `spotify-2023_copy`;


select *
from `spotify-2023_copy2`
where row_num >1;

# we made our new table so we can delate the duplictes 

delete
from `spotify-2023_copy2`
where row_num >1;


select *
from `spotify-2023_copy2`;

#2 step stnadardize the data 

 select distinct track_name
 from `spotify-2023_copy2`
 order by 1;
 

 update `spotify-2023_copy2`
 set track_name= trim(track_name);
 
 
 
 select distinct track_name,`artist(s)_name`
 from `spotify-2023_copy2`
 order by 2;


UPDATE `spotify-2023_copy2`
SET track_name = REGEXP_REPLACE(track_name, '[^a-zA-Z0-9\\s\\?\\(\\)]', '')
WHERE track_name REGEXP '[^a-zA-Z0-9\\s\\?\\(\\)]';

 select distinct track_name,`artist(s)_name`
 from `spotify-2023_copy2`
 where `artist(s)_name`= "taylor swift"
 order by 1;
 
 
 select track_name
 from `spotify-2023_copy2`
 where track_name like "%(taylors ve";
 

 
UPDATE `spotify-2023_copy2`
SET track_name = REPLACE(track_name, '(Taylors Ve', '(Taylors Version)')
WHERE track_name LIKE '%taylors ve';

 select track_name
 from `spotify-2023_copy2`
 where `artist(s)_name` = "taylor swift" ;
 
 
 UPDATE `spotify-2023_copy2`
SET track_name = REPLACE(track_name, 'Dont Bl', 'Don’t Blame Me')
WHERE track_name LIKE '%Dont Bl%';
 
  select track_name
 from `spotify-2023_copy2`
 where `artist(s)_name` = "taylor swift" ;
 
  select track_name
 from `spotify-2023_copy2`
 where track_name like "%(from the" ;
 
 UPDATE `spotify-2023_copy2`
SET track_name = REPLACE(track_name, '(From The', '(From The Vault)')
WHERE track_name LIKE "%(from the" ; 

  select track_name
 from `spotify-2023_copy2`
 where `artist(s)_name` = "taylor swift" ;
 
 
select track_name
from `spotify-2023_copy2`;

select distinct `artist(s)_name`
from `spotify-2023_copy2`
order by 1;

UPDATE `spotify-2023_copy2`
SET `artist(s)_name` = REGEXP_REPLACE(`artist(s)_name`, '[^a-zA-Z0-9\\s,]', '')
WHERE `artist(s)_name` REGEXP '[^a-zA-Z0-9\\s,]';


select distinct `artist(s)_name`
from `spotify-2023_copy2`
order by 1;

update `spotify-2023_copy2`
set `artist(s)_name` = trim(`artist(s)_name`);

update `spotify-2023_copy2`
set `artist(s)_name` = upper(`artist(s)_name`);


select distinct `artist(s)_name`
from `spotify-2023_copy2`
order by 1;

select *
from `spotify-2023_copy2`
;


ALTER TABLE `spotify-2023_copy2`
ADD release_date DATE;

UPDATE `spotify-2023_copy2`
SET release_date = DATE(CONCAT(released_year, '-', released_month, '-', released_day));

select *
from `spotify-2023_copy2`
;

 #3 step deal with null values
 
 
 update `spotify-2023_copy2`
 set track_name = null
 where track_name = "";
 
 
 update `spotify-2023_copy2`
 set `artist(s)_name` = null
 where `artist(s)_name` = "";
 
 select *
 from `spotify-2023_copy2`
 where track_name is null or `artist(s)_name` is null;
 
 delete 
 from `spotify-2023_copy2`
  where track_name is null ;
  
SELECT *
FROM `spotify-2023_copy2`
WHERE `key` IS NULL or `key` = "";

update `spotify-2023_copy2`
set `key` = null
WHERE `key` ='';
-- no more null values


#4 remvoing useless columns 

ALTER TABLE `spotify-2023_copy2`
DROP COLUMN released_year,
DROP COLUMN released_month,
DROP COLUMN released_day,
DROP COLUMN bpm,
DROP COLUMN  `mode`,
DROP COLUMN `danceability_%`,
DROP COLUMN `valence_%`,
DROP COLUMN `energy_%`,
DROP COLUMN `acousticness_%`,
DROP COLUMN `instrumentalness_%`,
DROP COLUMN row_num,
DROP COLUMN `liveness_%`,
DROP COLUMN `speechiness_%`;


 select *
 from `spotify-2023_copy2`;
 

 
 