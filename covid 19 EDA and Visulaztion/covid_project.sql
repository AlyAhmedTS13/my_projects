SELECT *
FROM covid_deaths
where continent is not null   
ORDER BY 3,4;

ALTER TABLE covid_deaths
MODIFY COLUMN `date` DATE;

ALTER TABLE covid_deaths
ADD COLUMN new_date DATE;

UPDATE covid_deaths
SET new_date = STR_TO_DATE(date, '%m/%e/%Y');

SELECT date, new_date FROM covid_death LIMIT 10;

select location,new_date,total_cases,new_cases,total_deaths,population
FROM covid_deaths
ORDER BY 1,2 ;

ALTER TABLE covid_deaths
DROP COLUMN date;

ALTER TABLE covid_deaths
CHANGE COLUMN new_date date DATE;



select location ,`date`,new_cases,total_cases,total_deaths,population
FROM covid_death
ORDER BY 1,2 ;


-- Looking at total cases vs total deaths 
-- shis shows the likelihood of dying if you get covid in your country 
select location ,`date`,total_cases,total_deaths,(total_deaths/total_cases) *100 as percentage
FROM covid_deaths
where location like '%Egypt%'
ORDER BY 1,2 ;


-- looking at the total cases vs population 
-- shows what percentage got covid in your country each day
select location ,`date`,total_cases, population,(total_cases/population)*100 as percentage
FROM covid_deaths
where location like '%Egypt%' and continent is not null
ORDER BY 1,2 ;


-- shows what percentage got covid in your country overall
select location,population ,max(total_cases) as highest_infection,(max(total_cases)/population)*100 as percentage
FROM covid_deaths
where continent is not null
group by location,population
order by percentage desc
limit 10 ;

--  countries with highest death count

select location,max(total_deaths) as highest_death
FROM covid_deaths
where continent is not null
group by location
order by  highest_death desc;

--  countient with highest death count
select location,max(total_deaths) as highest_death
FROM covid_deaths
where continent is null and location != "world"
group by location
order by  highest_death desc;

-- global numbers for whole world 
select `date` , sum(new_cases) as total_cases , sum(new_deaths) as total_deaths , (sum(new_deaths)/sum(new_cases))*100 as percentage
from covid_deaths
where continent is not null
group by `date`
order by 1;

ALTER TABLE covid_vacc
ADD COLUMN new_date DATE;

UPDATE covid_vacc
SET new_date = STR_TO_DATE(date, '%m/%e/%Y');

ALTER TABLE covid_vacc
DROP COLUMN date;

ALTER TABLE covid_vacc
CHANGE COLUMN new_date date DATE;

select *
from covid_vacc;

select *
from covid_deaths;


select t1.location as location ,population,t1.date,t2.new_vaccinations, sum(t2.new_vaccinations)  over( partition by t1.location order by t1.date ) as total_vacc
from covid_deaths as t1
join covid_vacc as t2 
	on t1.location = t2.location 
    and
     t1.date = t2.date
where t1.continent is not null
order by 1,3;


-- looking at total population vs vaccination
with temp as (select t1.location as location ,population,t1.date,t2.new_vaccinations, sum(t2.new_vaccinations)  over( partition by t1.location order by t1.date ) as total_vacc
from covid_deaths as t1
join covid_vacc as t2 
	on t1.location = t2.location 
    and
     t1.date = t2.date
where t1.continent is not null
order by 1,3)
select location,population,max(total_vacc) as total_vacc,(max(total_vacc)/population)*100 as percentage
from temp
group by location,population
order by 1;

-- do this by a temp table
drop table if exists temp1;
create temporary table temp1 as
select t1.location as location ,population,t1.date,t2.new_vaccinations, sum(t2.new_vaccinations)  over( partition by t1.location order by t1.date ) as total_vacc
from covid_deaths as t1
join covid_vacc as t2 
	on t1.location = t2.location 
    and
     t1.date = t2.date
where t1.continent is not null
order by 1,3;

select location,population,max(total_vacc) as total_vacc,(max(total_vacc)/population)*100 as percentage
from temp1
group by location,population
order by 1;

-- creating view to save a query for later viusalization

create view view1 as 
select t1.location as location ,population,t1.date,t2.new_vaccinations, sum(t2.new_vaccinations)  over( partition by t1.location order by t1.date ) as total_vacc
from covid_deaths as t1
join covid_vacc as t2 
	on t1.location = t2.location 
    and
     t1.date = t2.date
where t1.continent is not null;

select *
from view1
order by 1,3;

select max(total_deaths)
from covid_deaths;

select sum(new_deaths)
from covid_deaths
where continent is not null;

