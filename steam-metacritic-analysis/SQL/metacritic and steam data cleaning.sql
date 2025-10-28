# cleaning our data from both data sets 

SELECT *
from metacritic_toppc_games;

select *
from steam_popular_games;

# we will start with metacritic data
SELECT *
from metacritic_toppc_games;

# change release Date inot DATE data type

update metacritic_toppc_games
set release_Date = str_to_date(release_Date,'%b %d, %Y');

select release_Date
from metacritic_toppc_games;

alter table metacritic_toppc_games
modify column release_Date date;

# remove 'Metascore' from score column and change data type to int

update metacritic_toppc_games
set score = replace(score,'Metascore','');

alter table metacritic_toppc_games
modify column score int;

select score
from metacritic_toppc_games;

# replace 'null' values in Rating with 'N/A' and score with 0

update metacritic_toppc_games
set rating = COALESCE(rating,'N/A');

select distinct rating
from metacritic_toppc_games;

update metacritic_toppc_games
set score = coalesce(score,0);

select distinct score
from metacritic_toppc_games;

# now we clean the steam data 

select *
from steam_popular_games;

# deal with null values in developer and pupblisher

update steam_popular_games
set developer = coalesce(developer,'N/A'),
publisher = coalesce(publisher,'N/A');

# remove columns that have many null values and we dont want 

alter table steam_popular_games
drop column score_rank,
drop column userscore;


# change data types of price columns

alter table steam_popular_games
modify column price double,
modify column initialprice double,
modify column discount int;

# convert price columns to price in dollars ( its in cents ) 

select truncate(price/100 ,2) , truncate(initialprice/100 ,2) 
from steam_popular_games;



update steam_popular_games
set price = truncate(price/100 ,2),
initialprice = truncate(initialprice/100 ,2);


select price,initialprice
from steam_popular_games;

#change column names to reflect the data better


alter table steam_popular_games
change column price price_USD double,
change column initialprice initialprice_USD double,
change column discount `discount_%` int,
change column ccu peak_Current_Players_yesterday int,
change column average_forever average_playTime_total int,
change column average_2weeks average_playTime_2weeks int,
change column median_forever median_playTime_total int,
change column median_2weeks median_platyime_2weeks int;

select *
from steam_popular_games;

# we will make a new column and find the estimate avarage number of owners of game

select  (cast(substring_index(replace(owners,',',''),' .. ',1)as unsigned) +
		cast(substring_index(replace(owners,',',''),' .. ',-1)as unsigned))/2 as avg
from steam_popular_games;

alter table steam_popular_games
add column estimated_owners bigint;

update steam_popular_games
set estimated_owners =(cast(substring_index(replace(owners,',',''),' .. ',1)as unsigned) +
		cast(substring_index(replace(owners,',',''),' .. ',-1)as unsigned))/2
;

select *
from steam_popular_games;

alter table steam_popular_games
change column owners owners_range varchar(26);


# now our data is clean for both data sets and ready for EDA

# we will export the clean data 

SELECT * FROM metacritic_toppc_games;
select* from steam_popular_games;



