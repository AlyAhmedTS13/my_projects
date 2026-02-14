-- EDA 
select*
from customers
limit 10;

select*
from `events`
limit 10;

select *
from `orders`
limit 10;

-- 1 What is our total revenue by month?

select sum((order_value-discount_amount)) as revenue, month(order_date) as `month`
from orders
group by `month`
order by `month` asc;

-- How many active customers do we have each month?
select count( distinct customer_id) as num_customers_active, month(event_date) as `month`
from `events`
group by `month`
order by `month`;

-- What is the average order value overall and by account_type (free vs premium)?
select round(avg(order_value),1) avg_order_value , account_type
from customers as t1
join orders as t2
	on t1.customer_id=t2.customer_id
group by account_type with rollup;

-- What is the failed payment rate by month?
select round(sum(case when payment_status = 'failed' then 1 end)*100/count(*) ,2) as failed_rate, 
month(order_date) as `month`
from orders
group by `month`
order by failed_rate;


--  Which acquisition_channel brings the highest total revenue?
select acquisition_channel , sum((order_value-discount_amount)) as revenue
from customers as t1
join orders as t2
	on t1.customer_id=t2.customer_id
group by acquisition_channel
order by revenue desc;

-- What percentage of active users make at least one purchase in a given month?
select round(count( distinct case when event_type = 'purchase' then customer_id end ) *100 / 
count(distinct customer_id),2) as percentage,
month(event_date) as `month`
from events
group by `month`
order by percentage desc;

-- What is the average session duration by device_type?
select round(avg(session_duration_sec),2) as avg_session_duration_sec,device_type
from events
group by device_type;

-- Which product_category generates the highest revenue?

select product_category, sum((order_value-discount_amount)) as revenue
from orders 
group by product_category
order by revenue desc;

-- Do premium users generate more revenue because they buy more frequently or because they spend more per order?

select account_type,round(avg(order_value),2) as money_spent
from customers as t1
join orders as t2 
	on t1.customer_id=t2.customer_id
group by account_type
order by money_spent desc;

select account_type,round(avg(num_orders),2) as avg_orders_per_user
from(
select account_type,t1.cus tomer_id,count(*) as num_orders
from customers as t1
join orders as t2 
	on t1.customer_id=t2.customer_id
group by account_type,t1.customer_id) as t3
group by account_type;

-- Has customer engagement (session duration and event count) changed after month 9?

select round(avg(session_duration_sec),2) as session_duration ,
count(*) as event_count , timestampdiff(month,'2024-07-03',event_date)+1 as month_count
from events
group by month_count
order by month_count ;


-- Did discount usage increase over time, and did it impact revenue per customer?

select count(*) as count ,sum(discount_applied) as discount_usage,
round(sum(order_value-discount_amount)/count(distinct customer_id),2)  as revenue_per_customer, 
timestampdiff(month,'2024-07-10',order_date)+1 as month_count
from orders
group by month_count
order by month_count ;

-- Are mobile users converting (purchase rate) lower than web users?

select device_type, 
round(count(distinct case when event_type='purchase' then customer_id end)*100/count(distinct customer_id),2) as  
purchase_rate 
from `events`
group by device_type
order by purchase_rate;

 -- Which acquisition_channel brings customers with the highest 6-month revenue?
 select acquisition_channel, round(avg(revenue),2) as avg_revenue_6M_per_customer
 from(
 select acquisition_channel, t2.customer_id, sum(order_value-discount_amount) as revenue
 from customers as t1 
 join orders as t2
	on t1.customer_id=t2.customer_id
where order_date between '2024-07-10' and adddate('2024-07-10',interval 6 month)
group by acquisition_channel,t2.customer_id) as t3
group by acquisition_channel
order by avg_revenue_6M_per_customer desc;

 -- Are customers who experience failed payments less likely to purchase again?
select  case when failed_orders > 0 then "experienced_failed" else "never_failed" end as customer_group,
round(avg(success_orders),2) as avg_successful_orders_after
from
(select customer_id, sum(case when payment_status = "failed" then 1 else 0 end) as failed_orders,
sum(case when payment_status = "success" then 1 else 0 end) as success_orders
from orders
group by customer_id) t1
group by customer_group
order by avg_successful_orders_after desc;


-- Which regions show declining engagement but stable traffic?

select geo_country,round(avg(session_duration_sec),2) as avg_session_sec, count(distinct customer_id) as num_users,
timestampdiff(month,'2024-07-03',event_date)+1 as month_count
from `events`
group by geo_country,month_count
order by month_count , avg_session_sec desc;

-- How long does it take on average for a new customer to make their first purchase?
select round(avg(time_in_days),2) as avg_time_days
from(
select t2.customer_id ,datediff(order_date,signup_date) as time_in_days , 
row_number() over( partition by t2.customer_id order by datediff(order_date,signup_date) asc ) as row_num
from customers as t1
join orders as t2
	on t1.customer_id=t2.customer_id) as  sub1
where row_num = 1;

-- Did customer behavior change significantly after month 9? If yes, which metrics changed first?

select count(distinct t2.customer_id) as num_active_users,
round(avg(session_duration_sec),2) as avg_session_duration_sec,sum(order_value-discount_amount) as revenue,
count(order_id) as num_orders,
timestampdiff(month,'2024-07-10',order_date)+1 as month_count
from `events` as t1
join orders as t2
	on t1.customer_id=t2.customer_id
group by month_count
order by month_count asc;


SELECT 
    TIMESTAMPDIFF(MONTH, '2024-07-10', event_date) + 1 AS month_count,
    COUNT(DISTINCT customer_id) AS num_active_users,
    ROUND(AVG(session_duration_sec), 2) AS avg_session_duration_sec
FROM events
GROUP BY month_count
ORDER BY month_count ASC;


SELECT 
    TIMESTAMPDIFF(MONTH, '2024-07-10', order_date) + 1 AS month_count,
    COUNT(DISTINCT customer_id) AS num_buyers,
    COUNT(order_id) AS num_orders,
    SUM(order_value - discount_amount) AS revenue
FROM orders
GROUP BY month_count
ORDER BY month_count ASC;


-- Is revenue decline (if any) driven by fewer active customers or lower spending per customer?
select month_count,
count(distinct customer_id) as num_buyers,
round(avg(order_value_sum),2) as avg_order_value
from (
select timestampdiff(MONTH, '2024-07-10', order_date) + 1 AS month_count,
round(sum(order_value),2) as order_value_sum, 
customer_id
from orders
group by customer_id,month_count)as sub1
group by month_count
order by month_count asc;

-- Are we becoming more dependent on discounts to maintain revenue?

select timestampdiff(month,'2024-07-10', order_date) +1 as month_count,
sum(order_value-discount_amount) as revenue,
sum(discount_applied) as num_of_discounts_applied
from orders
group by month_count
order by month_count;

-- Are high-value customers reducing engagement before reducing spending?
create view top_10_spenders as 
select timestampdiff(month,'2024-07-10', order_date) +1 as month_count,
customer_id , sum(order_value-discount_amount) as revenue
from orders
where customer_id in (
select  customer_id 
from
(select customer_id,
sum(order_value-discount_amount) as revenue
from orders
group by customer_id
order by revenue desc
limit 10) as t1
) 
group by customer_id,month_count
order by customer_id,month_count;

create view top_customers_events as
select timestampdiff(month,'2024-07-3', event_date) +1 as month_count,
customer_id,
round(avg(session_duration_sec),2) as avg_session_sec
from `events`
where customer_id in (
select  customer_id 
from
(select customer_id,
sum(order_value-discount_amount) as revenue
from orders
group by customer_id
order by revenue desc
limit 10) as t1
) 
group by customer_id,month_count
order by customer_id,month_count;


select t1.customer_id,
t1.month_count,
t1.revenue,
t2.avg_session_sec
from top_10_spenders as t1
join top_customers_events as t2
	on t1.customer_id=t2.customer_id
and t1.month_count=t2.month_count;



