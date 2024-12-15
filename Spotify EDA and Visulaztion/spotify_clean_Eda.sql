-- EDA 
select min(release_date),max(release_date)
from `spotify-2023_copy2`;

select *
from`spotify-2023_copy2`
order by release_date asc
limit 1;

select *
from`spotify-2023_copy2`
order by release_date desc
limit 1;

-- we found the earliest track in our data released in 1930 and latest track in 2023 


select track_name,`artist(s)_name`,streams,release_date
from`spotify-2023_copy2`
order by 3 desc
limit 10;

-- top 10 songs according to streams where the top track got over 2 billion streams 


select track_name , `artist(s)_name`, count(track_name) over() as num_of_tracks
from `spotify-2023_copy2`
where in_spotify_charts=0;

# there are around 360 tracks that werent in  spotify charts 

with t1 as (
select track_name , `artist(s)_name`, count(track_name) over() as num_of_tracks
from `spotify-2023_copy2`
where in_spotify_charts=0
)
select track_name , `artist(s)_name`
from t1
where `artist(s)_name` = 'olivia rodrigo';


# we can use this code to look for any artist that hadnt had anysong in the charts 


select track_name , `artist(s)_name`, in_spotify_playlists
from `spotify-2023_copy2`
order by 3 desc
limit 10;

# top 10 tracks that were present the most in spotify playlists 

select year(release_date) as `year`, count(track_name) as count_of_tracks
from`spotify-2023_copy2`
group by 1
order by 1  desc ;

# 2022 is the year with the most tracks 


select track_name,`artist(s)_name`,streams,release_date,in_spotify_charts,in_spotify_playlists
from`spotify-2023_copy2`
where `artist(s)_name` = "taylor swift"
order by streams desc 
limit 10 ;

# top 10 songs by taylor swift according to streams 


select count(track_name)as num_of_tracks, `key`
from`spotify-2023_copy2`
group by 2
having `key` is not null
order by 1 desc;

-- most used key in songs is c#

select*
from`spotify-2023_copy2`
where track_name='die for you';