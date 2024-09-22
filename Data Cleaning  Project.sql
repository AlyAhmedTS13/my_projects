-- data cleaning in sql 
select *
from layoffs;
# steps to clean data 
-- 1st remove duplicates 
-- 2nd stnadardize the data 
-- 3rd deal with null values
-- 4th remove useless columns ( when we remove or update columns we dont want to do that to the raw original data base so we create a back up)

-- back up to work with 

create table layoffs_copy
like layoffs;

select *
from layoffs_copy;

insert layoffs_copy
select*
from layoffs;

select *
from layoffs_copy;

-- step 1 

with dup as 
(
select *, 
row_number() over(partition by company,location,industry,total_laid_off
,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from layoffs_copy

)
select*
from dup
where row_num >1 ;


# so we found duplicates but we cant delete them from the cte ( in my sql we cant) so we are gonna create a new table with row_num as a column and del rows where its >1


CREATE TABLE `layoffs_copy2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



insert into layoffs_copy2
select *, 
row_number() over(partition by company,location,industry,total_laid_off
,percentage_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from layoffs_copy;

select *
from layoffs_copy2
where row_num >1;

delete
from layoffs_copy2
where row_num >1;

select *
from layoffs_copy2;

-- step 2 

select company,trim(company), upper(company)
from layoffs_copy2;

update layoffs_copy2
set company=trim(company);

update layoffs_copy2
set company= upper(company);

select*
from layoffs_copy2;
# we made sure the company column doesnt have any white spaces and in all caps

select distinct industry
from layoffs_copy2
order by 1;

select *
from layoffs_copy2
where industry like 'crypto%';

update layoffs_copy2
set industry = 'crypto'
where industry like 'crypto%';


select *
from layoffs_copy2
where industry like 'crypto%';

# we made sure the industry column has no smiliar values we changed crypto and crypto currency into just 'crypto'

select distinct location
from layoffs_copy2
order by 1;

# this column is good

select distinct country
from layoffs_copy2
order by 1;

select *
from layoffs_copy2
where country like '%states.';

select  distinct country, trim(trailing '.' from country )
from layoffs_copy2
order by country;

update layoffs_copy2
set country = trim(trailing '.' from country )
where country like '%states.';

select  distinct country
from layoffs_copy2
order by 1;

# this column had an instance where there is a . we removed it we could have used the same way to change it as with the crypto one 

select *
from layoffs_copy2;

select distinct stage
from layoffs_copy2
order by  1;

# this column is good

select `date`
from layoffs_copy2;

select `date`,str_to_date(`date`,'%m/%d/%Y')
from layoffs_copy2;


update layoffs_copy2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from layoffs_copy2;

alter table layoffs_copy2
modify column `date` date;

select `date`
from layoffs_copy2;
# we needed to chnage the format of date from string to date time format so we can use it later in eda first we had to use the str_to_date to change format then change the data type from text to date 

# step 3 


select *
from layoffs_copy2
where total_laid_off is null and percentage_laid_off is null;
# we might remove all of this in step 4 since its all useless

select *
from layoffs_copy2
where industry is null or industry = '';
# we try to fill the empty from other columns

select*
from layoffs_copy2
where company='airbnb';


update layoffs_copy2
set industry = null
where industry='';

# first we need to change the blank to null so we able to compute missing data

select *
from layoffs_copy2 as t1
join layoffs_copy2 as t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;
# we did a self join to see if there are companies where the industry is null  in some instances and not in other instances 
# so we can compute the missing values using and update statment


update layoffs_copy2 as t1
join layoffs_copy2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_copy2
where industry is null or industry = '';

# only one row left where the industry in null cuz there was no other row we can compute it from

DELETE FROM layoffs_copy2
WHERE industry IS NULL;

select *
from layoffs_copy2
where industry is null or industry = '';

# industry column is clean now 

select *
from layoffs_copy2
where stage is null or stage ='';

select *
from layoffs_copy2 as t1
join layoffs_copy2 as t2
	on t1.company = t2.company
where (t1.stage is null or t1.stage ='')
and t2.stage is not null;

# we cant fill the nulls in stage columns 

select *
from layoffs_copy2
where country is null or country ='';

# no nulls in country 

select *
from layoffs_copy2
where location is null or location ='';

# no nulls in location 
# all columns are clean except for total laid off and percentage which we cant fill the empty cuz we have like no total column same for funds raised column

# step 4

select *
from layoffs_copy2
where total_laid_off is null and percentage_laid_off is null;

delete
from layoffs_copy2
where total_laid_off is null and percentage_laid_off is null;

# we removed all rows where total_laid_off and percentage_laid_off  is null cuz we wont be able to use this data in our use case

select *
from layoffs_copy2;
# row_num can be deleted since we wont use it 
ALTER TABLE layoffs_copy2
DROP COLUMN row_num;

select *
from layoffs_copy2;

# data is now ready for eda thanks!!
