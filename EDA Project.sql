# sometimes when you do eda you will find more things to clean eda and data cleaning can
# coincide sometimes

select *
from layoffs_copy2;


select max(total_laid_off),max(percentage_laid_off)
from layoffs_copy2;
#max laid off in one day was 12,000 and some compaines completely closed with a percentage of 100% laid off

select*,row_number() over() as num_of_companies
from layoffs_copy2
where percentage_laid_off=1
order by total_laid_off desc;
# 116 compaines got totally closed where 100% of its employies got laid off


select company, sum(total_laid_off) 
from layoffs_copy2
group by company
order by sum(total_laid_off) desc ;
# amazon was the company with the most total laid off accros all dates with a 18,150 employee

select min(`date`) , max(`date`)
from layoffs_copy2;
# the range of date in our data is almost 3 years 

select industry, sum(total_laid_off) 
from layoffs_copy2
group by industry
order by sum(total_laid_off) desc ;
# consumer and retail where the industries where the most laid offs happened


select country, sum(total_laid_off) 
from layoffs_copy2
group by country
order by sum(total_laid_off) desc ;
# usa was by far the country with the most laid offs in just 3 years

select year(`date`) as year, sum(total_laid_off) 
from layoffs_copy2
group by year(`date`)
order by year desc ;
# 2022 was the year with the most laid off but 2023 is also alot and we only have 3 months of data from this year!!


SELECT SUBSTRING(`date`, 1, 7) AS date, SUM(total_laid_off) 
FROM layoffs_copy2
WHERE date IS NOT NULL
GROUP BY SUBSTRING(`date`, 1, 7)
ORDER BY date;

with rolling_total as
(SELECT SUBSTRING(`date`, 1, 7) AS date, SUM(total_laid_off) as total_laid_off
FROM layoffs_copy2
WHERE date IS NOT NULL
GROUP BY SUBSTRING(`date`, 1, 7)
ORDER BY date
)
select `date`,total_laid_off, sum(total_laid_off) over ( order by `date`) as rolling_total
from rolling_total;
# we did a rolling total to see how many was the lay offs in each month of the year how much did it increased by


select company, year(`date`), sum(total_laid_off)
from layoffs_copy2
group by company,year(`date`)
order by 3 desc;


with company_year as
(select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
from layoffs_copy2
group by company,year(`date`)
),
company_year_rank as
(
select*,dense_rank() over(partition by years order by total_laid_off desc ) as `rank`
from company_year
where years is not null
)

select *
from company_year_rank
where `rank` <= 5;

# we did a rank to find the top 5 companies how did a lay off in each year 
# thank you

