# Q1: How does price change with Year?

select `year` , round(avg(Price_EGP),0) as avg_price
from cars_new
group by `year`
order by avg_price ;

# Q2: What is the relationship between Engine Capacity (CC) and Price?
select case
when `Engine Capacity CC` <=1300 
 then 'Small'
when `Engine Capacity CC` >1300 and `Engine Capacity CC` <= 1600
 then 'Medium'
when `Engine Capacity CC` >1600 and `Engine Capacity CC` <= 2000 
 then 'Upper-Medium'
when `Engine Capacity CC` >2000 and `Engine Capacity CC` <= 3000 
 then 'Large'
when `Engine Capacity CC` >3000 
 then 'Very Large'
 end as engine_Capacity, 
round(avg(Price_EGP),0) as avg_price
from cars_new
group by  engine_Capacity
order by avg_price ; 

/*Category	    CC Range	        Meaning
Small	        <= 1300 cc	        Compact / economic cars_new
Medium	        1301 – 1600 cc	    The most common class (sedans, hatchbacks)
Upper-Medium    1601 – 2000 cc	    Higher performance sedans / SUVs
Large	        2001 – 3000 cc	    large SUVs
Very Large	    > 3000 cc	        Luxury / sports */


# Q3: Which Body Types are most expensive?

select `Body Type`, round(avg(Price_EGP),0) as avg_price
from cars_new
group by `Body Type`
order by 2;

# Q4: Which brands have the highest average price?

select Brand, round(avg(Price_EGP),0) as avg_price
from cars_new
group by Brand
order by 2 desc
limit 10;

# Q5: Which brands have the lowest average price?

select Brand, round(avg(Price_EGP),0) as avg_price
from cars_new
group by Brand
order by 2 
limit 10;


# Q6: Which model within each brand holds value the best?
select Brand,Model,avg_price
from
(select  Brand,Model,round(avg(Price_EGP),0) as avg_price, row_number() over(partition by Brand order by round(avg(Price_EGP),0) desc ) as rn
from cars_new
group by  Brand,Model)as t1
where rn =1 
order by avg_price desc ;

# Q7: Which brands/models are most common in the dataset?
select Brand,Model, count(*) num_cars_new,round(avg(Price_EGP),0)as avg_price
from cars_new
group by Brand,Model
order by 3 desc;

# Q8: Does Transmission Type influence price? (Automatic vs manual)

select `Transmission Type`, round(avg(Price_EGP),0) as avg_price
from cars_new
group by `Transmission Type`
order by 2 ;

# Q9: Which fuel types are most expensive?

select `Fuel Type`, round(avg(Price_EGP),0) as avg_price
from cars_new
group by `Fuel Type`
order by 2 ;

# Q10: Which negative conditions reduce price the most??
create view cars_new_negative_conditions as
SELECT 
    'accident' AS feature,
    ROUND(AVG(CASE WHEN accident = 1 THEN Price_EGP END),0) AS avg_negative,
    ROUND(AVG(CASE WHEN accident = 0 THEN Price_EGP END),0) AS avg_positive
FROM cars_new
UNION ALL
SELECT 
    'rasha',
    ROUND(AVG(CASE WHEN rasha = 1 THEN Price_EGP END),0),
    ROUND(AVG(CASE WHEN rasha = 0 THEN Price_EGP END),0)
FROM cars_new
UNION ALL
SELECT 
    'high_km',
    ROUND(AVG(CASE WHEN kilometers > 150000 THEN Price_EGP END),0),
    ROUND(AVG(CASE WHEN kilometers <= 150000 THEN Price_EGP END),0)
FROM cars_new;

select * ,truncate((`avg_positive`-`avg_negative`)/(`avg_positive`) *100,1) as diff_in_price
from cars_new_negative_conditions
order by diff_in_price desc;


# Q11: What features increase price the most?


create view cars_new_features as 
SELECT 
    'sunroof' AS feature,
    ROUND(AVG(CASE WHEN sunroof = 1 THEN Price_EGP END),0) AS avg_included,
    ROUND(AVG(CASE WHEN sunroof = 0 THEN Price_EGP END),0) AS avg_not_included
from cars_new


UNION ALL
    
    SELECT 
    'screen' ,
    ROUND(AVG(CASE WHEN screen = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN screen = 0 THEN Price_EGP END),0) 
from cars_new

UNION ALL

    SELECT 
    'sensors' ,
    ROUND(AVG(CASE WHEN sensors = 1 THEN Price_EGP END),0)  ,
    ROUND(AVG(CASE WHEN sensors = 0 THEN Price_EGP END),0) 
from cars_new

UNION ALL

    SELECT 
    'rear_camera',
    ROUND(AVG(CASE WHEN rear_camera = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN rear_camera = 0 THEN Price_EGP END),0) 
from cars_new


UNION ALL

    SELECT 
    'cruise' ,
    ROUND(AVG(CASE WHEN cruise = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN cruise = 0 THEN Price_EGP END),0) 
from cars_new


UNION ALL

    SELECT 
    'alloy' ,
    ROUND(AVG(CASE WHEN alloy = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN alloy = 0 THEN Price_EGP END),0) 
from cars_new

UNION ALL

    SELECT 
    'push_start' ,
    ROUND(AVG(CASE WHEN push_start = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN push_start = 0 THEN Price_EGP END),0) 
from cars_new


UNION ALL

    SELECT 
    'abs' ,
    ROUND(AVG(CASE WHEN abs = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN abs = 0 THEN Price_EGP END),0) 
from cars_new

UNION ALL

    SELECT 
    'airbags' ,
    ROUND(AVG(CASE WHEN airbags = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN airbags = 0 THEN Price_EGP END),0) 
from cars_new

UNION ALL

    SELECT 
    'air_condition' ,
    ROUND(AVG(CASE WHEN air_condition = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN air_condition = 0 THEN Price_EGP END),0) 
from cars_new

UNION ALL

    SELECT 
    'bluetooth' ,
    ROUND(AVG(CASE WHEN bluetooth = 1 THEN Price_EGP END),0) ,
    ROUND(AVG(CASE WHEN bluetooth = 0 THEN Price_EGP END),0) 
from cars_new
order by avg_included desc;

select * ,truncate((`avg_included`-`avg_not_included`)/(`avg_not_included`) *100,1) as diff_in_price
from cars_new_features
order by diff_in_price desc;


# Q12: Does Zero Condition correlate with higher price

select zero_condition,round(avg(Price_EGP),0) as avg_price
from cars_new
group by zero_condition
order by avg_price desc;


# Q13: Which brands/models have the lowest supply but high prices?
select Brand,Model, count(*) as supply_count, ROUND(AVG(Price_EGP),0) AS avg_price
from cars_new 
where Price_EGP >= 500000
group by Brand,Model
having count(*) <=10
ORDER BY supply_count ASC, avg_price DESC;

# Q14: Which body type has the highest feature richness?

SELECT 
    `Body Type`,
    ROUND(AVG(
        (ABS) +
        (Airbags) +
        (Sunroof) +
        (cruise) +
        (sensors) +
        (Bluetooth) +
        (rear_camera)+
        (push_start)+
        (alloy)+
        (screen)+
        (air_condition)
    ), 2) AS avg_feature_count
FROM cars_new
GROUP BY `Body Type`
ORDER BY avg_feature_count DESC;

# Q15: How do Kilometers affect Price?
SELECT 
    FLOOR(Kilometers / 100000) * 100000 AS km_bin,
    ROUND(AVG(Price_EGP), 0) AS avg_price,
    COUNT(*) AS car_count
FROM cars_new
GROUP BY km_bin
ORDER BY km_bin;


