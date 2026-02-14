-- feature engineering
create view Monetary_Features as
select customer_id, sum( case when payment_status = 'success' then order_value end) as 
total_spent,
sum(case when payment_status = 'success' then 1 else 0 end ) as 
orders_count,
round(avg( case when payment_status = 'success' then order_value end),2) as avg_order_value,
sum(discount_applied)/count(*) as discount_usage_rate,
sum(case when payment_status = 'failed' then 1 else 0 end )/count(*)as
failed_payment_rate,
sum(case when payment_status = 'refunded' then 1 else 0 end )/count(*) as
refund_rate,
round(avg(case when payment_status = 'success' then items_count end),2) as avg_items_per_order
from orders
group by customer_id
order by customer_id;

create view Engagement_Features as
select customer_id,count(event_id) as total_events,
count(distinct event_date) as active_days,
round(avg(session_duration_sec),2) as avg_session_duration,
count(distinct session_id) as total_sessions,
sum( case when event_type = 'purchase' then 1 else 0 end) as purchase_events,
sum( case when event_type = 'browse' then 1 else 0 end) as browse_events
from `events`
group by customer_id
order by customer_id;

create view last_event as
select customer_id,datediff(curdate(),max(event_date)) as days_since_last_event
from `events`
group by customer_id
order by customer_id;

create view last_purchase as
select customer_id,datediff(curdate(),max(order_date)) as days_since_last_purchase
from orders
where payment_status= 'success'
group by customer_id
order by customer_id;

create view tenure_days as 
select t1.customer_id,datediff(curdate(),signup_date) as customer_tenure_days
from customers as t1 
join `events` as t2
	on t1.customer_id=t2.customer_id
group by t1.customer_id;



create view customers_std as 
select t1.customer_id,round(stddev(session_duration_sec),4) as session_duration_std,
round(stddev(order_value),4) as order_value_std
from `events` as t1
join orders as t2
	on t1.customer_id=t2.customer_id
group by t1.customer_id;

create view event_change as 
SELECT customer_id,
COUNT(*) - LAG(COUNT(event_id)) OVER (PARTITION BY customer_id order by TIMESTAMPDIFF(MONTH,'2024-07-03',event_date)+1
) AS monthly_event_change,
TIMESTAMPDIFF(MONTH,'2024-07-03',event_date)+1 AS month_count
FROM events
GROUP BY customer_id, month_count
ORDER BY month_count, customer_id;


CREATE VIEW spend_change AS
SELECT customer_id,
SUM(order_value - discount_amount)-LAG(SUM(order_value - discount_amount)) 
OVER (PARTITION BY customer_id 
ORDER BY TIMESTAMPDIFF(MONTH,'2024-07-10', order_date) + 1) AS monthly_spend_change,
TIMESTAMPDIFF(MONTH,'2024-07-10', order_date) + 1 AS month_count
FROM orders
WHERE payment_status = 'success'
GROUP BY customer_id, month_count
ORDER BY month_count, customer_id;


create view avg_event_change as 
SELECT customer_id,
round(avg(monthly_event_change),3) AS avg_monthly_event_change
FROM event_change
GROUP BY customer_id;


create view avg_spend_change as 
SELECT customer_id,
round(avg(monthly_spend_change),3) AS avg_monthly_spend_change
FROM spend_change
GROUP BY customer_id;


CREATE TABLE ml_model_features AS
SELECT 
    c.customer_id,
    c.signup_date,
    c.birth_year,
    c.gender,
    c.country,
    c.region,
    c.acquisition_channel,
    c.account_type,

    -- Monetary
    m.total_spent,
    m.orders_count,
    m.avg_order_value,
    m.discount_usage_rate,
    m.failed_payment_rate,
    m.refund_rate,
    m.avg_items_per_order,

    -- Engagement
    e.total_events,
    e.active_days,
    e.avg_session_duration,
    e.total_sessions,
    e.purchase_events,
    e.browse_events,

    -- Recency & Tenure
    le.days_since_last_event,
    lp.days_since_last_purchase,
    t.customer_tenure_days,

    -- Volatility / Stability
    cs.session_duration_std,
    cs.order_value_std,
    avg_event.avg_monthly_event_change,
    avg_change.avg_monthly_spend_change
    

FROM customers c
LEFT JOIN Monetary_Features m
    ON c.customer_id = m.customer_id
LEFT JOIN Engagement_Features e
    ON c.customer_id = e.customer_id
LEFT JOIN last_event le
    ON c.customer_id = le.customer_id
LEFT JOIN last_purchase lp
    ON c.customer_id = lp.customer_id
LEFT JOIN tenure_days t
    ON c.customer_id = t.customer_id
LEFT JOIN customers_std cs
    ON c.customer_id = cs.customer_id
left join avg_event_change avg_event
	on avg_event.customer_id=c.customer_id
left join avg_spend_change avg_change
	on avg_change.customer_id=c.customer_id;


select *
from ml_model_features

